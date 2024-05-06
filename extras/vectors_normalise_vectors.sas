/*-----------------------------------------------------------------------------------------*
   Vector normalisation code

   This code accepts a set of vector embeddings (columns), checks whether they are normalised,
   and if necessary, normalises them to a unit vector (magnitude 1).

   Input parameters:
   - inputTable_NAME_BASE: table reference which contains the vectors. Do not quote.
   - inputTable_lib: SAS libref to the input table location. Do not quote.
   - colPattern: a pattern denoting the columns containing the vectors.  Don't add wildcards
                  such as * or %.  This program takes care of the same.  Do not quote.

   Output specifications:
   - outputTable_name: table reference to normalised vector output
   - outputTable_lib: SAS libref to the output table location

   - Sundaresh Sankaran, 3rd May 2024

*------------------------------------------------------------------------------------------*/

%let inputTable_NAME_BASE=SCORED_TOPICS;
%let inputTable_lib=PUBLIC;
%let colPattern=_Col;

%let outputTable_NAME=TEMP;
%let outputTable_lib=PUBLIC;


/*-----------------------------------------------------------------------------------------*
   START MACRO DEFINITIONS.
*------------------------------------------------------------------------------------------*/

/* -------------------------------------------------------------------------------------------* 
   Macro to initialize a run-time trigger global macro variable to run SAS Studio Custom Steps. 
   A value of 1 (the default) enables this custom step to run.  A value of 0 (provided by 
   upstream code) sets this to disabled.

   Input:
   1. triggerName: The name of the runtime trigger you wish to create. Ensure you provide a 
      unique value to this parameter since it will be declared as a global variable.

   Output:
   2. &triggerName : A global variable which takes the name provided to triggerName.
*-------------------------------------------------------------------------------------------- */

%macro _create_runtime_trigger(triggerName);

   %global &triggerName.;

   %if %sysevalf(%superq(&triggerName.)=, boolean)  %then %do;
  
      %put NOTE: Trigger macro variable &triggerName. does not exist. Creating it now.;
      %let &triggerName.=1;

   %end;

%mend _create_runtime_trigger;


/* -----------------------------------------------------------------------------------------* 
   Macro to create an error flag for capture during code execution.

   Input:
      1. errorFlagName: The name of the error flag you wish to create. Ensure you provide a 
         unique value to this parameter since it will be declared as a global variable.
      2. errorFlagDesc: The name of an error flag description variable to hold the error 
         description.

    Output:
      1. &errorFlagName : A global variable which takes the name provided to errorFlagName.
      2. &errorFlagDesc : A global variable which takes the name provided to errorFlagDesc.

*------------------------------------------------------------------------------------------ */

%macro _create_error_flag(errorFlagName, errorFlagDesc);

   %global &errorFlagName.;
   %global &errorFlagDesc.;
   %let &errorFlagName.=0;
   %let &errorFlagDesc. = No errors reported so far;

%mend _create_error_flag;


/*-----------------------------------------------------------------------------------------*
   Macro to check if a given libref belongs to a SAS or CAS engine.

   Input:
   1. sasCasLibref: a libref to be checked. Do not quote.
   2. tableEngine: a flag to hold the table Engine value.
   3. errorFlagName: a flag to populate an error code with.
   4. errorFlagDesc: a flag to describe the error if one occurs.
   5. sessionExists: an indicator (1) whether an active CAS session exists.  If not(0),
                     it will be created.
                     
   Output:
   1. tableEngine: populated with SAS or CAS
   2. errorFlagName: populated with 1 if an error and 0 if not
   3. errorFlagDesc: populated in case of an error
*------------------------------------------------------------------------------------------*/

%macro _sas_or_cas(sasCasLibref, tableEngine, errorFlagName, errorFlagDesc, sessionExists);

   %if &sessionExists. = 0 %then %do;
      cas _temp_ss_ ;
      caslib _ALL_ assign;
   %end;

    proc sql noprint;
        select distinct Engine into:&&tableEngine. from dictionary.libnames where libname = upcase("&sasCasLibref.");
    quit;

    %if %sysfunc(compress("&&&tableEngine.")) = "V9" %THEN %DO;
        data _null_;
            call symput("&tableEngine.","SAS");
            call symputx("&errorFlag.",0);
            call symput("&errorFlagDesc.","");
        run;
    %end;
    %else %if %sysfunc(compress("&&&tableEngine.")) = "CAS" %THEN %DO;
        data _null_;
            call symputx("&errorFlagName.",0);
            call symput("&errorFlagDesc.","");
        run;
    %END;
    %else %do;
        data _null_;
            call symputx("&errorFlagName.",1);
            call symput("&errorFlagDesc.","Unable to associate libref with either SAS or CAS. Check the input libref provided.");
        run;
    %end;

   %if &sessionExists. = 0 %then %do;
      cas _temp_ss_ terminate;
   %end;
    

%mend _sas_or_cas;


/*-----------------------------------------------------------------------------------------*
   Macro to capture indicator and UUIDof any currently active CAS session.
   UUID is not expensive and can be used in future to consider graceful reconnect.

   Input:
   1. errorFlagName: name of an error flag that gets populated in case the connection is 
                     not active. Provide this value in quotes when executing the macro.
                     Define this as a global macro variable in order to use downstream.
   2. errorFlagDesc: Name of a macro variable which can hold a descriptive message output
                     from the check.
                     
   Output:
   1. Informational note as required. We explicitly don't provide an error note since 
      there is an easy recourse(of being able to connect to CAS)
   2. UUID of the session: macro variable which gets created if a session exists.
   3. errorFlagName: populated
   4. errorFlagDesc: populated
*------------------------------------------------------------------------------------------*/

%macro _env_cas_checkSession(errorFlagName, errorFlagDesc);
    %if %sysfunc(symexist(_current_uuid_)) %then %do;
       %symdel _current_uuid_;
    %end;
    %if %sysfunc(symexist(_SESSREF_)) %then %do;
      %let casSessionExists= %sysfunc(sessfound(&_SESSREF_.));
      %if &casSessionExists.=1 %then %do;
         %global _current_uuid_;
         %let _current_uuid_=;   
         proc cas;
            session.sessionId result = sessresults;
            call symputx("_current_uuid_", sessresults[1]);
         quit;
         %put NOTE: A CAS session &_SESSREF_. is currently active with UUID &_current_uuid_. ;
         data _null_;
            call symputx(&errorFlagName., 0);
            call symput(&errorFlagDesc., "CAS session is active.");
         run;
      %end;
      %else %do;
         %put NOTE: Unable to find a currently active CAS session. Reconnect or connect to a CAS session upstream. ;
         data _null_;
            call symputx(&errorFlagName., 1);
            call symput(&errorFlagDesc., "Unable to find a currently active CAS session. Reconnect or connect to a CAS session upstream.");
        run;
      %end;
   %end;
   %else %do;
      %put NOTE: No active CAS session ;
      data _null_;
        call symputx(&errorFlagName., 1);
        call symput(&errorFlagDesc., "No active CAS session. Connect to a CAS session upstream.");
      run;
   %end;
%mend _env_cas_checkSession;


/*-----------------------------------------------------------------------------------------*
   EXECUTION CODE MACRO 

   _nv prefix stands for Normalise Vector
*------------------------------------------------------------------------------------------*/

%macro _nv_execution_code;

   %put NOTE: Starting main execution code;

/*-----------------------------------------------------------------------------------------*
   Create an error flag. 
*------------------------------------------------------------------------------------------*/

   %_create_error_flag(_nv_error_flag, _nv_error_desc);

   %put NOTE: Error flag created;

/*-----------------------------------------------------------------------------------------*
   Check for an active CAS session
*------------------------------------------------------------------------------------------*/
   %if &_nv_error_flag. = 0 %then %do;
      %_env_cas_checkSession("_nv_error_flag", "_nv_error_desc");
      %let sessionExist = %sysfunc(sum(1, -1 * &_nv_error_flag.));
   %end;

   %let _nv_error_flag = 0;
/*-----------------------------------------------------------------------------------------*
   Check if the engine's SAS or CAS
*------------------------------------------------------------------------------------------*/
   %let _input_engine=;
   %let _output_engine=;

   %if &_nv_error_flag. = 0 %then %do;
      %_sas_or_cas(&inputTable_lib, _input_engine, _nv_error_flag, _nv_error_desc, &sessionExist. );
   %end;

   %if &_nv_error_flag. = 0 %then %do;
      %put NOTE: The input table location refers to a &_input_engine. engine.;
      %_sas_or_cas(&inputTable_lib, _output_engine, _nv_error_flag, _nv_error_desc, &sessionExist. );
   %end;

   %if &_nv_error_flag. = 0 %then %do;
      %put NOTE: The output table location refers to a &_output_engine. engine.;
   %end;   

   %if &sessionExist.=0 %then %do;
      %if &_nv_error_flag. = 0 %then %do;
         %if %sysfunc(compress("&_input_engine.")) = "CAS" %then %do;
            data _null_;
               call symputx("_nv_error_flag",1);
               call symput("_nv_error_desc","A CAS session does not exist even though input table runs on CAS. Connect to a CAS session upstream.");
            run;
         %end;
      %end;
      
      %if &_nv_error_flag. = 0 %then %do;
         %if %sysfunc(compress("&_output_engine.")) = "CAS" %then %do;
            data _null_;
               call symputx("_nv_error_flag",1);
               call symput("_nv_error_desc","A CAS session does not exist even though output table runs on CAS. Connect to a CAS session upstream.");
            run;
         %end;
      %end;
   %end;

   %if &_nv_error_flag. = 0 %then %do;

      proc sql noprint;
/*-----------------------------------------------------------------------------------------*
   Create a string of all columns conforming to the vector pattern.
*------------------------------------------------------------------------------------------*/
        select name into:colString separated by " " from dictionary.columns
           where libname="&inputTable_lib." and memname = "&inputTable_NAME_BASE." 
              and upcase(name) like upcase("%"||"&colPattern.%");
/*-----------------------------------------------------------------------------------------*
   Create a string of all new normalized columns.
*------------------------------------------------------------------------------------------*/
        select cats("norm_",name) into:normColString separated by " " from dictionary.columns
           where libname="&inputTable_lib." and memname = "&inputTable_NAME_BASE." 
              and upcase(name) like upcase("%"||"&colPattern.%");
      quit;

      %put NOTE: The columns are &colString.;
      %put NOTE: New columns to construct are &normColString.;

   %end;

   %if "&colString." = "" %then %do;
      data _null_;
         call symputx("_nv_error_flag",1);
         call symput("_nv_error_desc","No columns of the given pattern can be found. Check input parameters.");
      run;
   %end;

   %if &_nv_error_flag. = 0 %then %do;

      data &outputTable_lib..&outputTable_NAME (drop = i);
         length &normColString. 8. sum_squares norm normalized_flag 8.;

      set &inputTable_lib..&inputTable_NAME_BASE;

         array vect(*) &colString.;
/*-----------------------------------------------------------------------------------------*
   Calculate the norm of a vector, which is the square root of sum of squares of individual
   vectors.
*------------------------------------------------------------------------------------------*/
         do i = 1 to sum(count("&colString"," "),1);
            sum_squares = sum(sum_squares,vect(i)**2);
         end;
         if sum_squares > 0 then norm = SQRT(sum_squares);

         array normvec(*)  &normColString.;
         do i = 1 to sum(count("&colString"," "),1);
/*-----------------------------------------------------------------------------------------*
   Vectors which already resemble unit vectors (plus or minus 0.001) needn't be normalized
   again.
*------------------------------------------------------------------------------------------*/
               if norm >= 0.999 and norm <= 1.001 then do;
                  normvec(i) = vect(i);
                  normalized_flag = 0;
               end;
               else do;
                  if sum_squares > 0 then normvec(i) = vect(i)/norm;
                  else normvec(i) = vect(i);
                  normalized_flag = 1;
               end;
         end;
      run; 
   %end;


%mend _nv_execution_code;
   
/*-----------------------------------------------------------------------------------------*
   EXECUTION CODE
   The execution code is controlled by the trigger variable defined in this custom step. This
   trigger variable is in an "enabled" (value of 1) state by default, but in some cases, as 
   dictated by logic, could be set to a "disabled" (value of 0) state.
*------------------------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------------------*
   Create run-time trigger. 
*------------------------------------------------------------------------------------------*/

%_create_runtime_trigger(_nv_run_trigger);

/*-----------------------------------------------------------------------------------------*
   Execute 
*------------------------------------------------------------------------------------------*/

%if &_nv_run_trigger. = 1 %then %do;

   %_nv_execution_code;

%end;

%if &_nv_run_trigger. = 0 %then %do;

   %put NOTE: This step / code has been disabled.  Nothing to do.;

%end;


%put NOTE: Final summary;
%put NOTE: Status of error flag - &_nv_error_flag. ;
%put NOTE: Description of last encountered error - &_nv_error_desc. ;


/*-----------------------------------------------------------------------------------------*
   Clean up existing macro variables and macro definitions.
*------------------------------------------------------------------------------------------*/

%if %symexist(_input_engine) %then %do;
   %symdel _input_engine;
%end;
%if %symexist(_output_engine) %then %do;
   %symdel _output_engine;
%end;
%if %symexist(_current_uuid_) %then %do;
   %symdel _current_uuid_;
%end;

/*-----------------------------------------------------------------------------------------*
   This block is valid only for a standalone SAS program. Not required when this code is 
   used inside a custom step.
*------------------------------------------------------------------------------------------*/

%if %symexist(inputTable_NAME_BASE) %then %do;
   %symdel inputTable_NAME_BASE;
%end;
%if %symexist(inputTable_lib) %then %do;
   %symdel inputTable_lib;
%end;
%if %symexist(colPattern) %then %do;
   %symdel colPattern;
%end;
%if %symexist(outputTable_name) %then %do;
   %symdel outputTable_NAME;
%end;
%if %symexist(outputTable_lib) %then %do;
   %symdel outputTable_lib;
%end;