/******************************************************
Purpose:  The purpose of this query is to help you identify transactions that are stuck in limbo and might
          be locking tables up so you can then terminate them easily.


How:      It searches for all 'ExclusiveLock' process IDs (queries) for the current user that are 
          still showing as an active session, excluding the current one. (svv_transactions is amazing and
          manages to include the transaction itself in it's own query results, so we have to filter that 
          one out.)

Useage:   The output will be 2 fields, txn_start, the timestamp that the transaction was started, and term_txn_command
          which can be copied and pasted into your sql workbench environment for easy execution. 
          

*******************************************************/

SELECT txn_start 
    , 'SELECT pg_terminate_backend('||pid||');' AS term_txn_command
FROM svv_transactions 
WHERE lock_mode = 'ExclusiveLock' 
    AND txn_owner = CURRENT_USER
    AND pid <> pg_backend_pid()
;
