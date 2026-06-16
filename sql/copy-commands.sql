COPY airlines.airport_dim
FROM 's3://airlines-landing-data-proj/airports-dim/airports.csv'
IAM_ROLE 'arn:aws:iam::784230180132:role/ReadshiftS3readrole'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';
