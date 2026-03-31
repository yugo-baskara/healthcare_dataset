-- ================================================== --
-- Healthcare Data Analytics Pipeline (MySQL) 		  --
-- Author : Yugo B									  --
-- Data Source : Prasad Patil						  --
-- End-to-end data cleaning, validation, and analysis --
-- ================================================== --


-- ================ --
-- Create Raw Table --
-- ================ --

create table if not exists portofolio.healthcare_dataset_raw
(
patient_id bigint auto_increment primary key,
name varchar (100),
age int check (age between 0 and 120),
gender varchar (30),
blood_type varchar (5),
medical_condition varchar (100),
date_of_admission varchar(20),
doctor varchar (100),
hospital varchar (150),
insurance_provider varchar (100),
billing_amount decimal (10,2) check (billing_amount >= 0),
room_number int,
admission_type varchar (50),
discharge_date varchar(20),
medication varchar (100),
test_results varchar (20)
)
;


-- ==================== --
-- Load Data Into Table --
-- ==================== --

load data infile
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/healthcare_dataset.csv'
into table
portofolio.healthcare_dataset_raw
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\n'
ignore 1 rows
(
@name,
age,
gender,
blood_type,
medical_condition,
date_of_admission,
doctor,
hospital,
insurance_provider,
billing_amount,
room_number,
admission_type,
discharge_date,
medication,
test_results
)
set name = @name
;

-- NOTE
-- Please update file path based on your local environtment


-- ================== --
-- Create Clean Table --
-- ================== --

create table portofolio.healthcare_dataset_clean as
select
	patient_id,
	trim(upper(name)) as name,
    age,
    case
    when upper(gender) in ('M', 'MALE') then 'MALE'
    when upper(gender) in ('F', 'FEMALE') then 'FEMALE'
    else 'UNKNOWN'
    end as gender,
	blood_type,
	trim(upper(medical_condition)) as medical_condition,
	case
		when date_of_admission regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
		then str_to_date(date_of_admission, '%Y-%m-%d')
        else null
	end as admission_date,
	trim(upper(doctor)) as doctor,
	trim(upper(hospital)) as hospital,
	trim(upper(insurance_provider)) as insurance_provider,
	billing_amount,
	room_number,
	trim(upper(admission_type)) as admission_type,
	case
		when discharge_date regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        then str_to_date(discharge_date, '%Y-%m-%d') 
        else null
	end as discharge_date,
	trim(upper(medication)) as medication,
	trim(upper(test_results)) as test_results	
from portofolio.healthcare_dataset_raw
;


-- ============ --
-- Create Index --
-- ============ --

create index idx_hospital on portofolio.healthcare_dataset_clean(hospital);
create index idx_condition on portofolio.healthcare_dataset_clean(medical_condition);
create index idx_admission_date on portofolio.healthcare_dataset_clean(admission_date);


-- ========================================= --
-- Audit Data, Duplicate and Data Validation --
-- ========================================= --

select
	count(*)
from
	portofolio.healthcare_dataset_clean
;


select
	count(*) as total_rows,
	sum(case when name is null then 1 else 0 end) as null_name
from
	portofolio.healthcare_dataset_clean
;


select
	name, admission_date, count(*) as repeat_cust
from
	portofolio.healthcare_dataset_clean
group by
	name, admission_date
having count(*) > 1 
;


select
	*
from
	portofolio.healthcare_dataset_clean
where age < 0 or age > 120
;


select
	count(*) as null_billing
from
	portofolio.healthcare_dataset_clean
where
	billing_amount is null
;


-- =============== --
-- Date Validation --
-- =============== --

select
	*
from
	portofolio.healthcare_dataset_clean
where
	discharge_date < admission_date
;


-- =============== --
-- Outlier Billing --
-- =============== --

select
	*
from
	portofolio.healthcare_dataset_clean
where
	billing_amount < 0
order by
	billing_amount asc
;


-- =========================== --
-- Total Patient By Demography --
-- =========================== --

select
	gender,
	count(*) as total_patient
from
	portofolio.healthcare_dataset_clean
group by
	gender
;


-- ================ --
-- Revenue Analysis --
-- ================ --

select
	hospital,
    sum(billing_amount) as total_revenue,
    avg(billing_amount) as avg_revenue
from
	portofolio.healthcare_dataset_clean
group by
	hospital
order by
	total_revenue desc
;


-- ==================== --
-- Length Of Stay (LOS) --
-- ==================== --

select
	hospital,
    avg(
		case 
			when discharge_date is not null
				and admission_date is not null
			then datediff(discharge_date, admission_date) end) as avg_los
from
	portofolio.healthcare_dataset_clean
group by
	hospital
order by
	avg_los desc
;


-- ================================ --
-- High Frequency Medical Condition --
-- ================================ --

select
	medical_condition,
    count(*) as total_cases
from
	portofolio.healthcare_dataset_clean
group by
	medical_condition
order by
	total_cases desc
;


-- ================ --
-- Medication Usage --
-- ================ --

select
	medication,
    count(*) as usage_medication
from
	portofolio.healthcare_dataset_clean
group by
	medication
order by
	usage_medication desc
;


-- ================== --
-- Abnormal Test Rate --
-- ================== --

select
	hospital,
    sum(case when test_results = 'ABNORMAL' then 1 else 0 end) as abnormal_cases,
    count(*) as total_cases,
    (sum(case when test_results = 'ABNORMAL' then 1 else 0 end) * 100.0 / nullif(count(*),0)) as abnormal_rate
from
	portofolio.healthcare_dataset_clean
group by
	hospital
order by
	abnormal_rate desc
;


-- ==================== --
-- Revenue By Condition --
-- ==================== --

select
	medical_condition,
    total_revenue,
case
	when avg_billing < 1000 then 'LOW'
    when avg_billing between 1000 and 5000 then 'MEDIUM'
    else 'HIGH'
end as billing_category
from (select 
		medical_condition,
        sum(billing_amount) as total_revenue,
        avg(billing_amount) as avg_billing
from portofolio.healthcare_dataset_clean
		group by medical_condition ) Y
order by
	total_revenue desc
;


-- ========= --
-- Audit Log --
-- ========= --

create table portofolio.audit_log
(
	audit_id int auto_increment primary key,
	check_name varchar(100),
	issue_count int,
	created_at timestamp default current_timestamp
)
;


insert into audit_log (check_name, issue_count)
select
	'invalid date',
	count(*)
from
	portofolio.healthcare_dataset_clean
where
	discharge_date < admission_date
;
