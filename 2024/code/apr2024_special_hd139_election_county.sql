-----------------------------------------------------------------------------------------------
-- Before this process I am downloading the JSON file from GA elections and running it through
-- the enhanced_voting_process/load_json.ipynb Jupyter Notebook...
-- As I work through this process a couple of times I will create some documentation around it.
-----------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------
-- Load JSON data...
-----------------------------------------------------------------------------------------------
create or replace table raw.apr2024_special_hd139_election_county
as
select *
from read_json('/home/skunkworks/development/openelections-data-ga/2024/code/ga_20240409_county_level_data.json');

select *
from raw.apr2024_special_hd139_election_county;

-----------------------------------------------------------------------------------------------
-- Rename vote_types before pivoting the data...
-----------------------------------------------------------------------------------------------
select distinct vote_type
from raw.apr2024_special_hd139_election_county
order by vote_type;

update raw.apr2024_special_hd139_election_county
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
create or replace table stage.apr2024_special_hd139_election_county
as
pivot raw.apr2024_special_hd139_election_county
on vote_type
using sum(votes);

select *
from stage.apr2024_special_hd139_election_county;

alter table stage.apr2024_special_hd139_election_county
    add column precinct varchar;

alter table stage.apr2024_special_hd139_election_county
    add column district varchar;

select *
from stage.apr2024_special_hd139_election_county;

update stage.apr2024_special_hd139_election_county
    set precinct = 'not available';

select *
from stage.apr2024_special_hd139_election_county;

------------------------------------------------------------------------------------------------------------------------
-- Cleanup OFFICE...
------------------------------------------------------------------------------------------------------------------------
select
    office,
    count(distinct county) as num_counties,
    count(distinct candidate) as num_candidates
from stage.apr2024_special_hd139_election_county
group by office
order by office;

-- STEP #1 - need to find the offices we are going to pull out. We are only looking at Federal
--           and State offices right now. See the readme file in github for a list of them.
--           I typically take the above output and put it in a Google sheet and review them there.

alter table stage.apr2024_special_hd139_election_county
    add column original_office varchar;

update stage.apr2024_special_hd139_election_county
    set original_office = office;

select *
from stage.apr2024_special_hd139_election_county;

update stage.apr2024_special_hd139_election_county
    set office = 'State House',
        district = '139'
where office = 'State House of Representatives - District 139 ';

select distinct office, district
from stage.apr2024_special_hd139_election_county;

------------------------------------------------------------------------------------------------------------------------
-- Cleanup PARTY...
------------------------------------------------------------------------------------------------------------------------
select *
from stage.apr2024_special_hd139_election_county;

alter table stage.apr2024_special_hd139_election_county
column party type varchar;

update stage.apr2024_special_hd139_election_county
    set party = 'Republican'
where candidate in ('Sean Knox','Carmen Rice','Donald Moeller');

update stage.apr2024_special_hd139_election_county
    set party = 'Independent'
where candidate = 'Robert Mallard';


select candidate, party, count(*) as cnt
from stage.apr2024_special_hd139_election_county
group by candidate, party
order by party,candidate;

------------------------------------------------------------------------------------------------------------------------
-- Cleanup COUNTY...
------------------------------------------------------------------------------------------------------------------------
select county, count(*) as cnt
from stage.apr2024_special_hd139_election_county
group by county
order by county;

update stage.apr2024_special_hd139_election_county
    set county = replace(county, ' County', '');

------------------------------------------------------------------------------------------------------------------------
-- Cleanup CANDIDATE...
------------------------------------------------------------------------------------------------------------------------
alter table stage.apr2024_special_hd139_election_county
    add column original_candidate varchar;

update stage.apr2024_special_hd139_election_county
    set original_candidate = candidate;

-- select candidate, count(*) as cnt
-- from stage.apr2024_special_hd139_election_county
-- group by candidate;
--
-- update stage.apr2024_special_hd139_election_county
--     set candidate = trim(replace(candidate, ' (Rep)', ''))
-- where office in ('State House', 'State Senate');

select candidate, original_candidate, count(*) as cnt
from stage.apr2024_special_hd139_election_county
where office in ('State House', 'State Senate')
group by candidate, original_candidate
order by candidate;

-----------------------------------------------------------------------------------------------
-- Move data to PROD and QC the data...
-----------------------------------------------------------------------------------------------
create or replace table prod.apr2024_special_hd139_election_county
as
select *
from stage.apr2024_special_hd139_election_county
where office in ('State House')
order by office, party, candidate;

select *
from prod.apr2024_special_hd139_election_county;

-- Make sure vote type counts match total_votes...
select
    candidate,
    total_votes,
    (absentee_by_mail_votes + advanced_votes + election_day_votes + provisional_votes) as qc_total_votes
from prod.apr2024_special_hd139_election_county
where total_votes <> qc_total_votes;

-- Check a few race results with the website...
select
    office,
    district,
    candidate,
    party,
    sum(absentee_by_mail_votes + advanced_votes + election_day_votes + provisional_votes) as total_votes
from prod.apr2024_special_hd139_election_county
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
from prod.apr2024_special_hd139_election_county
where county = 'Harris'
order by county, office, district, candidate, party;

select candidate, count(*) as cnt
from prod.apr2024_special_hd139_election_county
group by candidate;

select *
from prod.apr2024_special_hd139_election_county;

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
    from prod.apr2024_special_hd139_election_county
    order by county, office, try_cast(district as integer), party, candidate   
) to '/home/skunkworks/development/openelections-data-ga/2024/20240409__ga__special__state__house__139__county-level.csv'
(HEADER, DELIMITER ',');

checkpoint;

