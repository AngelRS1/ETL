create schema if not exists analytics;

create table analytics.tbl_stg_dump_file
(
	id	varchar(36) encode zstd,
	title varchar(255) encode zstd,
	company varchar(255) encode zstd,
	sector varchar(255) encode zstd,
	city varchar(255) encode zstd,
	adverts_activedays integer encode zstd,
	adverts_applyurl  varchar(2500) encode zstd,
	adverts_id  varchar(36) encode zstd,
	adverts_publicationdatetime bigint encode zstd,
	adverts_status varchar(8) encode zstd,
	applicants_age integer encode zstd,
	applicants_applicationdate bigint encode zstd,
	applicants_firstname varchar(255) encode zstd,
	applicants_lastname varchar(255) encode zstd
);


create table analytics.Dim_Company
(
	Dim_company_id 	bigint     NOT NULL IDENTITY(1, 1),
	Company_name varchar(255) encode zstd,
	Company_sector varchar(255) encode zstd,
	Insert_date  timestamp encode zstd,
	PRIMARY KEY(Dim_company_id)
);

create table analytics.Dim_Job
(
	Dim_job_id 	bigint     NOT NULL IDENTITY(1, 1),
	Id varchar(36) encode zstd,
	Title varchar(255) encode zstd,
	Insert_date timestamp encode zstd,
	PRIMARY KEY(Dim_job_id)
);

create table analytics.Dim_Date
(
	Dim_date_id 	bigint     NOT NULL IDENTITY(1, 1),
	Fulldate date encode zstd,
	Day_of_week integer encode zstd,
	Day_of_monlth integer encode zstd,
	Day_of_year integer encode zstd,
	Week_of_year integer encode zstd,
	Month integer encode zstd,
	Year integer encode zstd,
	Insert_date timestamp encode zstd,
	PRIMARY KEY(Dim_date_id)
);


create table analytics.Dim_Status
(
	Dim_status_id 	bigint     NOT NULL IDENTITY(1, 1),
	Status varchar(8) encode zstd,
	Insert_date timestamp encode zstd,
	PRIMARY KEY(Dim_status_id)
);

create table analytics.Dim_Location
(
	Dim_location_id 	bigint     NOT NULL IDENTITY(1, 1),
	City varchar(255) encode zstd,
	Insert_date timestamp encode zstd,
	PRIMARY KEY(Dim_location_id)
);

create table analytics.Dim_Advert
(
	Dim_advert_id 	bigint     NOT NULL IDENTITY(1, 1),
	Id varchar(36) encode zstd,
	Apply_url varchar(2500) encode zstd,
	Insert_date timestamp encode zstd,
	PRIMARY KEY(Dim_advert_id)
);


create table analytics.Dim_Applicant
(
	Dim_applicant_id 	bigint     NOT NULL IDENTITY(1, 1),
	First_name varchar(255) encode zstd,
	Last_name varchar(255) encode zstd,
	Insert_date timestamp encode zstd,
	PRIMARY KEY(Dim_applicant_id)
);


create table analytics.Fact_job_advert
(
	Fact_job_advert_id 	bigint     NOT NULL IDENTITY(1, 1),
	Dim_advert_id bigint encode zstd,
	Dim_compay_id bigint encode zstd,
	Dim_job_id bigint encode zstd,
	Dim_status_id bigint encode zstd,
	Dim_location_id bigint encode zstd,
	Publication_date_id bigint encode zstd,
	Active_days integer encode zstd,
	Insert_date timestamp encode zstd,
	PRIMARY KEY(Fact_job_advert_id)
);

create table analytics.Fact_job_applicant
(
	Fact_job_applicant_id 	bigint     NOT NULL IDENTITY(1, 1),
	Dim_compay_id bigint encode zstd,
	Dim_job_id bigint encode zstd,
	Dim_location_id bigint encode zstd,
	Application_date_id bigint encode zstd,
	Dim_applicant_id bigint encode zstd,
	Age integer encode zstd,
	Insert_date timestamp encode zstd,
	PRIMARY KEY(Fact_job_applicant_id)
);

--- pupulate the dim_date which is static
INSERT INTO analytics.Dim_Date (Fulldate,Day_of_week,Day_of_monlth,Day_of_year,Week_of_year,Month,Year,Insert_date)
  SELECT
	 datum AS Fulldate
  , cast(to_char(datum, 'D') AS SMALLINT) AS Day_of_week
	, cast(extract(DAY FROM datum) AS SMALLINT) AS Day_of_monlth
	, cast(extract(DOY FROM datum) AS SMALLINT) AS Day_of_year
  ,extract(week from datum) as Week_of_year
	, cast(extract(MONTH FROM datum) AS SMALLINT) AS Month
	, cast(extract(YEAR FROM datum) AS SMALLINT) AS Year
  ,getdate()
  FROM
    (
      SELECT
        '2009-01-01' :: DATE + numbercol AS datum
      FROM (
           with seq_0_9 as (
select 0 as num
union all select 1 as num
union all select 2 as num
union all select 3 as num
union all select 4 as num
union all select 5 as num
union all select 6 as num
union all select 7 as num
union all select 8 as num
union all select 9 as num
), seq_0_999 as (
select a.num + b.num * 10 + c.num * 100 as num
from seq_0_9 a, seq_0_9 b, seq_0_9 c
)
select a.num + b.num * 1000 as numbercol
from seq_0_999 a, seq_0_999 b
order by numbercol
             ) numbers
      WHERE numbercol < 30 * 365 -- Generate 30 years
    ) DQ
  ORDER BY 1;

-- Querys to update the dim tables
--#############################################################################3
insert into analytics.Dim_Company (Company_name, Company_sector,Insert_date)
									select distinct Source.company,Source.sector,
									getdate()
									from
										(
										Select distinct company,sector
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Company Target
										On Target.Company_name = Source.company and Target.Company_sector = Source.sector
									Where Target.Company_name is null


insert into analytics.Dim_Job (Id, Title,Insert_date)
									select distinct Source.id,Source.title,
									getdate()
									from
										(
										Select distinct id,title
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Job Target
										On Target.Id = Source.id and Target.Title = Source.title
									Where Target.Id is null

insert into analytics.Dim_Status (Status, Insert_date)
									select distinct Source.adverts_status,
									getdate()
									from
										(
										Select distinct adverts_status
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Status Target
										On Target.Status = Source.adverts_status
									Where Target.Status is null

insert into analytics.Dim_Location (City, Insert_date)
									select distinct Source.City,
									getdate()
									from
										(
										Select distinct City
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Location Target
										On Target.City = Source.City
									Where Target.City is null

insert into analytics.Dim_Advert (Id, Apply_url,Insert_date)
									select distinct Source.adverts_id,Source.adverts_applyurl,
									getdate()
									from
										(
										Select distinct adverts_id,adverts_applyurl
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Advert Target
										On Target.Id = Source.adverts_id and Target.Apply_url = Source.adverts_applyurl
									Where Target.Id is null

insert into analytics.Dim_Applicant (First_name, Last_name,Insert_date)
									select distinct Source.applicants_firstname,Source.applicants_lastname,
									getdate()
									from
										(
										Select distinct applicants_firstname,applicants_lastname
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Applicant Target
										On Target.First_name = Source.applicants_firstname and Target.Last_name = Source.applicants_lastname
									Where Target.First_name is null

Insert into analytics.Fact_job_advert(Dim_advert_id,Dim_compay_id,Dim_job_id,Dim_status_id,Dim_location_id,Publication_date_id,Active_days,Insert_date)
          SELECT distinct
                AD.dim_advert_id,
                CMP.dim_company_id,
                J.dim_job_id,
                S.dim_status_id,
                L.dim_location_id,
                D.dim_date_id,
                Source.adverts_activedays,
                getdate()
          from analytics.tbl_stg_dump_file source
          LEFT JOIN analytics.Dim_advert AD
            on AD.Id = Source.adverts_id and AD.Apply_url = Source.adverts_applyurl
          LEFT JOIN analytics.Dim_Company CMP
            on CMP.Company_name = source.company and CMP.Company_sector = source.sector
          LEFT JOIN analytics.Dim_Job J
            on J.Id = source.id and J.Title = source.title
          LEFT JOIN analytics.Dim_Status S
            on S.Status = source.adverts_status
          LEFT JOIN analytics.Dim_Location L
            on L.City = source.city
          LEFT JOIN analytics.dim_Date D
            on D.Fulldate = (timestamp 'epoch' + source.adverts_publicationdatetime * interval '1 second')::date

Insert into analytics.Fact_job_applicant(Dim_compay_id,Dim_job_id,Dim_location_id,Application_date_id,Dim_applicant_id,Age,Insert_date)
          SELECT distinct
                CMP.dim_company_id,
                J.dim_job_id,
                L.dim_location_id,
                D.dim_date_id,
                AD.dim_applicant_id,
                Source.applicants_age,
                getdate()
          from analytics.tbl_stg_dump_file source
          LEFT JOIN analytics.Dim_Applicant AD
            on AD.First_name = Source.applicants_firstname and AD.Last_name = Source.applicants_lastname
          LEFT JOIN analytics.Dim_Company CMP
            on CMP.Company_name = source.company and CMP.Company_sector = source.sector
          LEFT JOIN analytics.Dim_Job J
            on J.Id = source.id and J.Title = source.title
          LEFT JOIN analytics.Dim_Status S
            on S.Status = source.adverts_status
          LEFT JOIN analytics.Dim_Location L
            on L.City = source.city
          LEFT JOIN analytics.dim_Date D
            on D.Fulldate = (timestamp 'epoch' + source.applicants_applicationdate * interval '1 second')::date