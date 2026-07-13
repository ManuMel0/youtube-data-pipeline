Bronze bucket name - yt-data-pipeline-bronze-444115535128-sa-east-1-an
Silver bucket name - yt-data-pipeline-silver-444115535128-sa-east-1-an
Gold bucket name - yt-data-pipeline-gold-444115535128-sa-east-1-an

Scripts bucket - yt-data-pipeline-scripts-444115535128-sa-east-1-an

SNS arn - arn:aws:sns:sa-east-1:444115535128:yt-data-pipeline-alerts

Glue bronze - yt_pipeline_bronze
Glue silver - yt_pipeline_silver
Glue gold - yt_pipeline_gold

--bronze_database yt_pipeline_bronze
--bronze_table raw_statistics
--silver_bucket yt-data-pipeline-silver-444115535128-sa-east-1-an
--silver_database yt_pipeline_silver
--silver_table clean_statistics
--gold_bucket yt-data-pipeline-gold-444115535128-sa-east-1-an
--gold_database yt_pipeline_gold