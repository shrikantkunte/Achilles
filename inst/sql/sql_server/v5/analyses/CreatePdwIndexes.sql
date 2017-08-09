IF 0 = (SELECT COUNT(*) as index_count
    FROM sys.indexes 
    WHERE object_id = OBJECT_ID('@resultsDatabaseSchema.ACHILLES_results')
    AND name='idx_ar_aid')
CREATE NONCLUSTERED INDEX idx_ar_aid
   ON @resultsDatabaseSchema.ACHILLES_results (analysis_id ASC);

IF 0 = (SELECT COUNT(*) as index_count
    FROM sys.indexes 
    WHERE object_id = OBJECT_ID('@resultsDatabaseSchema.ACHILLES_results')
    AND name='idx_ar_aid_s1')  
CREATE NONCLUSTERED INDEX idx_ar_aid_s1
   ON @resultsDatabaseSchema.ACHILLES_results (analysis_id ASC, stratum_1 ASC);

IF 0 = (SELECT COUNT(*) as index_count
    FROM sys.indexes 
    WHERE object_id = OBJECT_ID('@resultsDatabaseSchema.ACHILLES_results')
    AND name='idx_ar_aid_s1234')
CREATE NONCLUSTERED INDEX idx_ar_aid_s1234
   ON @resultsDatabaseSchema.ACHILLES_results (analysis_id ASC, stratum_1 ASC, stratum_2 ASC, stratum_3 ASC, stratum_4 ASC);
