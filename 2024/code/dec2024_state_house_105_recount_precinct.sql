-----------------------------------------------------------------------------------------------
-- Before this process I am downloading the JSON file from GA elections and running it through
-- the enhanced_voting_process/load_json.ipynb Jupyter Notebook...
-- As I work through this process a couple of times I will create some documentation around it.
-----------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------
-- Load JSON data...
-----------------------------------------------------------------------------------------------
create or replace table raw.dec2024_state_house_105_recount_precinct
as
select *
from read_json('/Users/skunkworks/Development/openelections-data-ga/2024/code/ga_20241203_precinct_level_data.json');

select *
from raw.dec2024_state_house_105_recount_precinct;

-- Checking on some of the county|precinct|candidates with 0 total votes...
select *
from raw.dec2024_state_house_105_recount_precinct
where coalesce(total_votes, 0) = 0;

-----------------------------------------------------------------------------------------------
-- Rename vote_types before pivoting the data...
-----------------------------------------------------------------------------------------------
select distinct vote_type
from raw.dec2024_state_house_105_recount_precinct
order by vote_type;

update raw.dec2024_state_house_105_recount_precinct
    set vote_type =
        case
            when vote_type = 'Absentee by Mail' then 'absentee_by_mail_votes'
            when vote_type = 'Advance Voting' then 'advanced_votes'
            when vote_type = 'Election Day' then 'election_day_votes'
            when vote_type = 'Provisional' then 'provisional_votes'
        end;


-----------------------------------------------------------------------------------------------
-- Pivot data and copy to STAGE, begin the cleanup and QC...
-----------------------------------------------------------------------------------------------
create or replace table stage.dec2024_state_house_105_recount_precinct
as
pivot raw.dec2024_state_house_105_recount_precinct
on vote_type
using sum(votes);

alter table stage.dec2024_state_house_105_recount_precinct
    add column district varchar;

select *
from stage.dec2024_state_house_105_recount_precinct;

------------------------------------------------------------------------------------------------------------------------
-- Cleanup OFFICE...
------------------------------------------------------------------------------------------------------------------------
select
    office,
    count(distinct county) as num_counties,
    count(distinct candidate) as num_candidates
from stage.dec2024_state_house_105_recount_precinct
group by office
order by office;

-- STEP #1 - need to find the offices we are going to pull out. We are only looking at Federal
--           and State offices right now. See the readme file in github for a list of them.
--           I typically take the above output and put it in a Google sheet and review them there.

alter table stage.dec2024_state_house_105_recount_precinct
    add column original_office varchar;

update stage.dec2024_state_house_105_recount_precinct
    set original_office = office;

select *
from stage.dec2024_state_house_105_recount_precinct;

select *
from stage.dec2024_state_house_105_recount_precinct
where office ilike 'State%House%Representatives%';

select distinct
    office,
    trim(split(office, ' - ')[1]) as new_office,
    trim(replace(split(office, ' - ')[2], 'District', '')) as district
from stage.dec2024_state_house_105_recount_precinct
where office ilike 'State%House%Representatives%'
order by office;

update stage.dec2024_state_house_105_recount_precinct
    set office = 'State House',
        district = '105'
where office ilike 'State%House%Representatives%';

select *
-- select count(distinct district)
from stage.dec2024_state_house_105_recount_precinct
where office = 'State House';

------------------------------------------------------------------------------------------------------------------------
-- Cleanup PARTY...
------------------------------------------------------------------------------------------------------------------------
select party, count(*) as cnt
from stage.dec2024_state_house_105_recount_precinct
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
group by party
order by party;

update stage.dec2024_state_house_105_recount_precinct
    set party = 'Democrat'
where upper(party) = 'DEM';

update stage.dec2024_state_house_105_recount_precinct
    set party = 'Republican'
where upper(party) = 'REP';

select party, count(*) as cnt
from stage.dec2024_state_house_105_recount_precinct
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
group by party
order by party;

select *
from stage.dec2024_state_house_105_recount_precinct
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
    and coalesce(party, '') = ''
order by candidate;

select party, count(*) as cnt
from stage.dec2024_state_house_105_recount_precinct
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
group by party
order by party;

------------------------------------------------------------------------------------------------------------------------
-- Cleanup COUNTY...
------------------------------------------------------------------------------------------------------------------------
select county, count(*) as cnt
from stage.dec2024_state_house_105_recount_precinct
group by county
order by county;

update stage.dec2024_state_house_105_recount_precinct
    set county = replace(county, ' County', '');

------------------------------------------------------------------------------------------------------------------------
-- Cleanup CANDIDATE...
------------------------------------------------------------------------------------------------------------------------
alter table stage.dec2024_state_house_105_recount_precinct
    add column original_candidate varchar;

update stage.dec2024_state_house_105_recount_precinct
    set original_candidate = candidate;

select *
from stage.dec2024_state_house_105_recount_precinct
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
limit 50;

select candidate, count(*) as cnt
from stage.dec2024_state_house_105_recount_precinct
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
group by candidate;

update stage.dec2024_state_house_105_recount_precinct
    set candidate = trim(replace(candidate, ' (Rep)', ''))
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House');

update stage.dec2024_state_house_105_recount_precinct
    set candidate = trim(replace(candidate, ' (Dem)', ''))
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House');

update stage.dec2024_state_house_105_recount_precinct
    set candidate = trim(replace(candidate, ' (I)', ''))
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House');

select candidate, original_candidate, count(*) as cnt
from stage.dec2024_state_house_105_recount_precinct
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
group by candidate, original_candidate
order by candidate;

-----------------------------------------------------------------------------------------------
-- Move data to PROD and QC the data...
-----------------------------------------------------------------------------------------------
create or replace table prod.dec2024_state_house_105_recount_precinct
as
select *
from stage.dec2024_state_house_105_recount_precinct
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
order by office, party, candidate;

select *
from prod.dec2024_state_house_105_recount_precinct;

-- Make sure vote type counts match total_votes...
select
    candidate,
    total_votes,
    (absentee_by_mail_votes + advanced_votes + election_day_votes + provisional_votes) as qc_total_votes
from prod.dec2024_state_house_105_recount_precinct
where total_votes <> qc_total_votes;

-- Check a few precinct race results with the website...
select
    office,
    district,
    candidate,
    party,
    sum(absentee_by_mail_votes + advanced_votes + election_day_votes + provisional_votes) as total_votes
from prod.dec2024_state_house_105_recount_precinct
group by office, district, candidate, party
order by office, district, candidate, party;

-- Check a few vote type counts with the website...
select
    county,
    precinct_name,
    office,
    district,
    candidate,
    party,
    advanced_votes,
    election_day_votes,
    absentee_by_mail_votes,
    provisional_votes
from prod.dec2024_state_house_105_recount_precinct
order by county, office, district, candidate, party;

-- Aggregate to county level and make sure we are still matching...
select
    county,
    office,
    district,
    candidate,
    party,
    sum(votes) as total_votes
from prod.dec2024_state_house_105_recount_precinct
group by county, office, district, candidate, party
order by county, office, district, candidate, party;

update prod.dec2024_state_house_105_recount_precinct
    set candidate = trim(candidate);
-----------------------------------------------------------------------------------------------
-- Write out CSV file...
-----------------------------------------------------------------------------------------------
COPY
(
    select
        county,
        precinct_name as precinct,
        office,
        district,
        party,
        candidate,
        election_day_votes,
        advanced_votes,
        absentee_by_mail_votes,
        provisional_votes
    from prod.dec2024_state_house_105_recount_precinct
    order by county, office, try_cast(district as integer), party, candidate   
) to '/Users/skunkworks/Development/openelections-data-ga/2024/20241203__ga__recount__state__house__105__precinct-level.csv'
(HEADER, DELIMITER ',');

checkpoint;

