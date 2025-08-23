-----------------------------------------------------------------------------------------------
-- Before this process I am downloading the JSON file from GA elections and running it through
-- the enhanced_voting_process/load_json.ipynb Jupyter Notebook...
-- As I work through this process a couple of times I will create some documentation around it.
-----------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------
-- Load JSON data...
-----------------------------------------------------------------------------------------------
create or replace table raw.may2024_genpri_election_county
as
select *
from read_json('/Users/skunkworks/Development/openelections-data-ga/2024/code/ga_20240521_county_level_data.json');

select *
from raw.may2024_genpri_election_county;

-----------------------------------------------------------------------------------------------
-- Rename vote_types before pivoting the data...
-----------------------------------------------------------------------------------------------
select distinct vote_type
from raw.may2024_genpri_election_county
order by vote_type;

update raw.may2024_genpri_election_county
    set vote_type =
        case
            when vote_type = 'Absentee by Mail Votes' then 'absentee_by_mail_votes'
            when vote_type = 'Advance Voting Votes' then 'advanced_votes'
            when vote_type = 'Election Day Votes' then 'election_day_votes'
            when vote_type = 'Provisional Votes' then 'provisional_votes'
        end;

-----------------------------------------------------------------------------------------------
-- Pivot data and copy to STAGE, begin the cleanup and QC...
-----------------------------------------------------------------------------------------------
create or replace table stage.may2024_genpri_election_county
as
pivot raw.may2024_genpri_election_county
on vote_type
using sum(votes);

select *
from stage.may2024_genpri_election_county;

-- Have to fix party JSON --> VARCHAR outside of Datagrip as it doesn't understand
-- the syntax correctly...

alter table stage.may2024_genpri_election_county
    add column precinct varchar;

alter table stage.may2024_genpri_election_county
    add column district varchar;

select *
from stage.may2024_genpri_election_county;

update stage.may2024_genpri_election_county
    set precinct = 'not available';

select *
from stage.may2024_genpri_election_county;

------------------------------------------------------------------------------------------------------------------------
-- Cleanup OFFICE...
------------------------------------------------------------------------------------------------------------------------
select
    office,
    count(distinct county) as num_counties,
    count(distinct candidate) as num_candidates
from stage.may2024_genpri_election_county
group by office
order by office;

-- STEP #1 - need to find the offices we are going to pull out. We are only looking at Federal
--           and State offices right now. See the readme file in github for a list of them.
--           I typically take the above output and put it in a Google sheet and review them there.

alter table stage.may2024_genpri_election_county
    add column original_office varchar;

update stage.may2024_genpri_election_county
    set original_office = office;

select *
from stage.may2024_genpri_election_county;

-- DISTRICT ATTORNEY...
select distinct
    office,
    trim(split(office, ' - ')[1]) as new_office,
    trim(split(office, ' - ')[2]) as district
from stage.may2024_genpri_election_county
where office ilike '%District%Attorney%'
order by office;

update stage.may2024_genpri_election_county
    set office = trim(split(office, ' - ')[1]),
        district = trim(split(office, ' - ')[2])
where office ilike '%District%Attorney%';

update stage.may2024_genpri_election_county
    set district = 'Gwinnett Judicial Circuit'
where office ilike '%District%Attorney%'
    and district = 'Gwinnett Judicial Circuit/ Para Fiscal del Distrito del  Circuito Judicial de Gwinnett';

select *
-- select distinct district
from stage.may2024_genpri_election_county
where office = 'District Attorney'
order by district;

-- APPEALS COURT JUDGE...
select distinct
    office,
    'Appeals Court Judge' as new_office,
    trim(replace(replace(trim(split(office, ' ')[-1]), '(', ''), ')', '')) as district
from stage.may2024_genpri_election_county
where office ilike '%Appeal%'
order by office;

update stage.may2024_genpri_election_county
    set office = 'Appeals Court Judge',
        district = trim(replace(replace(trim(split(office, ' ')[-1]), '(', ''), ')', ''))
where office ilike '%Appeal%';

select *
-- select distinct district
from stage.may2024_genpri_election_county
where office = 'Appeals Court Judge'
order by district;

-- SUPERIOR COURT JUDGE...
select distinct
    office,
    'Superior Court Judge' as new_office,
    trim(split_part(trim(split(office, ' - ')[3]), '(', 1)) as district
from stage.may2024_genpri_election_county
where office ilike '%Superior Court%'
order by office;

update stage.may2024_genpri_election_county
    set office = 'Superior Court Judge',
        district = trim(split_part(trim(split(office, ' - ')[3]), '(', 1))
where office ilike  '%Superior Court%';

select *
-- select distinct district
from stage.may2024_genpri_election_county
where office like '%Superior Court%'
order by district;

-- SUPREME COURT JUSTICE...
select distinct
    office,
    'Supreme Court Justice' as new_office,
    trim(replace(replace(trim(split(office, ' ')[-1]), '(', ''), ')', ''))  as district
from stage.may2024_genpri_election_county
where office ilike '%Supreme Court%'
order by office;

update stage.may2024_genpri_election_county
    set office = 'Supreme Court Justice',
        district = trim(replace(replace(trim(split(office, ' ')[-1]), '(', ''), ')', ''))
where office ilike  '%Supreme Court%';

-- STATE HOUSE...
select *
from stage.may2024_genpri_election_county
where office ilike 'State%House%Representatives%';

select distinct
    office,
    'State House' as new_office,
    trim(replace(split(split(office, '- ')[2], '/')[1], 'District', '')) as district
from stage.may2024_genpri_election_county
where office ilike '%State%House%'
order by office;

update stage.may2024_genpri_election_county
    set office = 'State House',
        district = trim(replace(split(split(office, '- ')[2], '/')[1], 'District', ''))
where office ilike 'State%House%Representatives%';

--STATE SENATE...
select *
from stage.may2024_genpri_election_county
where office ilike 'State%Senate%';

select distinct
    office,
    'State Senate' as new_office,
    trim(replace(split(split(office, '- ')[2], '/')[1], 'District', '')) as district
from stage.may2024_genpri_election_county
where office ilike 'State%Senate%'
order by office;

update stage.may2024_genpri_election_county
    set office = 'State Senate',
        district = trim(replace(split(split(office, '- ')[2], '/')[1], 'District', ''))
where office ilike 'State%Senate%';

-- U.S. HOUSE...
select *
from stage.may2024_genpri_election_county
where office ilike 'US House%';

select distinct
    office,
    'U.S. House' as new_office,
    trim(replace(split(split(office, '- ')[2], '/')[1], 'District', '')) as district
from stage.may2024_genpri_election_county
where office ilike 'US House%'
order by office;

update stage.may2024_genpri_election_county
    set office = 'U.S. House',
        district = trim(replace(split(split(office, '- ')[2], '/')[1], 'District', ''))
where office ilike 'US House%';

select distinct office, district
from stage.may2024_genpri_election_county
order by office;

delete from stage.may2024_genpri_election_county
where office ilike 'Party Question%';

select *
from stage.may2024_genpri_election_county
where district is null;

select office, district, count(*) as cnt
from stage.may2024_genpri_election_county
-- where office = 'State House'
group by office, district
order by office;

select *
from stage.may2024_genpri_election_county
where office = 'Appeals Court Judge'
    and district = 'Miller'
order by county;

------------------------------------------------------------------------------------------------------------------------
-- Cleanup PARTY...
------------------------------------------------------------------------------------------------------------------------
select party, count(*) as cnt
from stage.may2024_genpri_election_county
group by party
order by party;

update stage.may2024_genpri_election_county
    set party = 'Democrat'
where upper(party) = 'DEM';

update stage.may2024_genpri_election_county
    set party = 'Republican'
where upper(party) = 'REP';

select office, count(*) as cnt
from stage.may2024_genpri_election_county
where party is null
group by office;

select distinct candidate
from stage.may2024_genpri_election_county
where office = 'State House'
    and party is null;

update stage.may2024_genpri_election_county
    set party = 'Republican'
where candidate in ('Clay Griner','Angie O''Steen', 'Darlene K. Taylor (I)', 'William E. Roberts')
    and office = 'State House'
    and party is null;

update stage.may2024_genpri_election_county
    set party = 'Democrat'
where candidate in ('Theresa R. Rewis','Theresa Thomas', 'Dexter Sharper (I)')
    and office = 'State House'
    and party is null;

select distinct candidate
from stage.may2024_genpri_election_county
where office = 'District Attorney'
    and party is null;

update stage.may2024_genpri_election_county
    set party = 'Republican'
where candidate in ('Bryce Johnson (I)')
    and office = 'District Attorney'
    and party is null;

update stage.may2024_genpri_election_county
    set party = 'Nonpartisan'
where office in ('Appeals Court Judge', 'Superior Court Judge', 'Supreme Court Justice');

select *
from stage.may2024_genpri_election_county
where party is null;

select candidate, party, count(*) as cnt
from stage.may2024_genpri_election_county
group by candidate, party
order by party, candidate;

------------------------------------------------------------------------------------------------------------------------
-- Cleanup COUNTY...
------------------------------------------------------------------------------------------------------------------------
select county, count(*) as cnt
from stage.may2024_genpri_election_county
group by county
order by county;

update stage.may2024_genpri_election_county
    set county = replace(county, ' County', '');

------------------------------------------------------------------------------------------------------------------------
-- Cleanup CANDIDATE...
------------------------------------------------------------------------------------------------------------------------
alter table stage.may2024_genpri_election_county
    add column original_candidate varchar;

update stage.may2024_genpri_election_county
    set original_candidate = candidate;

select candidate, count(*) as cnt
from stage.may2024_genpri_election_county
group by candidate
order by candidate;

update stage.may2024_genpri_election_county
    set candidate = trim(replace(candidate, ' (I)', ''))
where candidate ilike '% (I)';

update stage.may2024_genpri_election_county
    set candidate = 'Brandon Faircloth'
where candidate = ' Brandon Faircloth';

select candidate, original_candidate, count(*) as cnt
from stage.may2024_genpri_election_county
where office in ('District Attorney', 'President', 'State House', 'State Senate', 'U.S. House')
group by candidate, original_candidate
order by candidate;

-----------------------------------------------------------------------------------------------
-- Move data to PROD and QC the data...
-----------------------------------------------------------------------------------------------
create or replace table prod.may2024_genpri_election_county
as
select *
from stage.may2024_genpri_election_county
order by office, party, candidate;

select *
from prod.may2024_genpri_election_county;

-- Make sure vote type counts match total_votes...
select
    candidate,
    total_votes,
    (absentee_by_mail_votes + advanced_votes + election_day_votes + provisional_votes) as qc_total_votes
from prod.may2024_genpri_election_county
where total_votes <> qc_total_votes;

-- Check a few race results with the website...
select
    office,
    district,
    candidate,
    party,
    sum(absentee_by_mail_votes + advanced_votes + election_day_votes + provisional_votes) as total_votes
from prod.may2024_genpri_election_county
group by office, district, candidate, party
order by office, district, candidate, party;

-- Check a few vote type counts with the website...
select
    county,
    office,
    district,
    candidate,
    party,
    advanced_votes,
    election_day_votes,
    absentee_by_mail_votes,
    provisional_votes
from prod.may2024_genpri_election_county
where county = 'Bartow'
order by county, office, district, candidate, party;

select candidate, count(*) as cnt
from prod.may2024_genpri_election_county
group by candidate
order by cnt desc;
-- Nobody should be above 159...

select *
from prod.may2024_genpri_election_county;

-----------------------------------------------------------------------------------------------
-- Write out CSV file...
-----------------------------------------------------------------------------------------------
COPY
(
    select
        county,
        precinct,
        office,
        district,
        party,
        candidate,
        election_day_votes,
        advanced_votes,
        absentee_by_mail_votes,
        provisional_votes
    from prod.may2024_genpri_election_county
    order by county, office, try_cast(district as integer), party, candidate   
) to '/Users/skunkworks/Development/openelections-data-ga/2024/20240521__ga__general__primary__county-level.csv'
(HEADER, DELIMITER ',');

checkpoint;
