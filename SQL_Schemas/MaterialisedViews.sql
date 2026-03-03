create materialized view public.batting_performance_mv as
with
  batting_innings as (
    select
      d.match_id,
      d.inning,
      d.batter_id,
      d.batting_team_id,
      d.bowling_team_id,
      m.season_id,
      m.match_date,
      m.match_type,
      case
        when d.inning <= 2 then 'regular'::text
        else 'super_over'::text
      end as innings_type,
      sum(COALESCE(d.batsman_runs::integer, 0)) as runs_scored,
      count(
        case
          when d.extras_type is null
          or (
            d.extras_type <> all (
              array[
                'wide'::extras_type_enum,
                'noball'::extras_type_enum
              ]
            )
          ) then 1
          else null::integer
        end
      ) as balls_faced,
      max(
        case
          when d.is_wicket
          and d.player_dismissed_id = d.batter_id then 1
          else 0
        end
      ) as was_dismissed,
      max(
        case
          when d.is_wicket
          and d.player_dismissed_id = d.batter_id then d.dismissal_kind
          else null::dismissal_enum
        end
      ) as dismissal_type,
      sum(
        case
          when d.batsman_runs = 4 then 1
          else 0
        end
      ) as fours,
      sum(
        case
          when d.batsman_runs = 6 then 1
          else 0
        end
      ) as sixes
    from
      deliveries d
      join matches m on d.match_id = m.match_id
    group by
      d.match_id,
      d.inning,
      d.batter_id,
      d.batting_team_id,
      d.bowling_team_id,
      m.season_id,
      m.match_date,
      m.match_type
  )
select
  bi.match_id,
  bi.inning,
  bi.batter_id,
  bi.batting_team_id,
  bi.bowling_team_id,
  bi.season_id,
  bi.match_date,
  bi.match_type,
  bi.innings_type,
  bi.runs_scored,
  bi.balls_faced,
  bi.was_dismissed,
  bi.dismissal_type,
  bi.fours,
  bi.sixes,
  p.player_name as batter_name,
  t1.team_name as batting_team,
  t2.team_name as bowling_team,
  s.season_year,
  round(
    bi.runs_scored::numeric * 100.0 / NULLIF(bi.balls_faced, 0)::numeric,
    2
  ) as strike_rate,
  case
    when bi.runs_scored >= 100 then 'century'::text
    when bi.runs_scored >= 50 then 'fifty'::text
    when bi.runs_scored = 0
    and bi.was_dismissed = 1 then 'duck'::text
    else 'normal'::text
  end as innings_category
from
  batting_innings bi
  join players p on bi.batter_id = p.player_id
  left join teams t1 on bi.batting_team_id = t1.team_id
  left join teams t2 on bi.bowling_team_id = t2.team_id
  join seasons s on bi.season_id = s.season_id;


create materialized view public.bowler_performance_mv as
with
  bowling_spells as (
    select
      d.match_id,
      d.inning,
      d.bowler_id,
      d.batting_team_id,
      m.season_id,
      m.match_date,
      m.match_type,
      case
        when d.inning <= 2 then 'regular'::text
        else 'super_over'::text
      end as innings_type,
      count(
        case
          when d.extras_type is null
          or (
            d.extras_type <> all (
              array[
                'wide'::extras_type_enum,
                'noball'::extras_type_enum
              ]
            )
          ) then 1
          else null::integer
        end
      ) as balls_bowled,
      count(*) as total_deliveries,
      sum(
        COALESCE(d.batsman_runs::integer, 0) + case
          when d.extras_type = 'wide'::extras_type_enum then COALESCE(d.extra_runs::integer, 0)
          when d.extras_type = 'noball'::extras_type_enum then 1
          else 0
        end
      ) as canonical_runs_conceded,
      sum(
        case
          when d.is_wicket = true
          and (
            d.dismissal_kind <> all (
              array[
                'run out'::dismissal_enum,
                'retired hurt'::dismissal_enum,
                'retired out'::dismissal_enum,
                'obstructing field'::dismissal_enum
              ]
            )
          ) then 1
          else 0
        end
      ) as wickets,
      sum(
        case
          when d.extras_type = 'wide'::extras_type_enum then 1
          else 0
        end
      ) as wides,
      sum(
        case
          when d.extras_type = 'noball'::extras_type_enum then 1
          else 0
        end
      ) as noballs,
      sum(
        case
          when COALESCE(d.total_runs::integer, 0) = 0 then 1
          else 0
        end
      ) as dot_balls,
      sum(
        case
          when d.batsman_runs = 4 then 1
          else 0
        end
      ) as fours_conceded,
      sum(
        case
          when d.batsman_runs = 6 then 1
          else 0
        end
      ) as sixes_conceded
    from
      deliveries d
      join matches m on d.match_id = m.match_id
    group by
      d.match_id,
      d.inning,
      d.bowler_id,
      d.batting_team_id,
      m.season_id,
      m.match_date,
      m.match_type
  ),
  corr as (
    select
      delivery_runs_corrections.match_id,
      delivery_runs_corrections.inning,
      delivery_runs_corrections.bowler_id,
      sum(delivery_runs_corrections.add_wide_runs)::integer as add_wide_runs
    from
      delivery_runs_corrections
    group by
      delivery_runs_corrections.match_id,
      delivery_runs_corrections.inning,
      delivery_runs_corrections.bowler_id
  )
select
  bs.match_id,
  bs.inning,
  bs.bowler_id,
  bs.batting_team_id,
  bs.season_id,
  bs.match_date,
  bs.match_type,
  bs.innings_type,
  bs.balls_bowled,
  bs.total_deliveries,
  (
    bs.canonical_runs_conceded + COALESCE(c.add_wide_runs, 0)
  )::integer as runs_conceded,
  bs.wickets,
  bs.wides,
  bs.noballs,
  bs.dot_balls,
  bs.fours_conceded,
  bs.sixes_conceded,
  p.player_name as bowler_name,
  t.team_name as batting_team,
  s.season_year,
  (
    floor(bs.balls_bowled::numeric / 6.0)::text || '.'::text
  ) || (bs.balls_bowled % 6::bigint) as overs_bowled,
  round(
    (
      bs.canonical_runs_conceded + COALESCE(c.add_wide_runs, 0)
    )::numeric * 6.0 / NULLIF(bs.balls_bowled, 0)::numeric,
    2
  ) as economy_rate,
  round(
    (
      bs.canonical_runs_conceded + COALESCE(c.add_wide_runs, 0)
    )::numeric / NULLIF(bs.wickets, 0)::numeric,
    2
  ) as bowling_average,
  round(
    bs.balls_bowled::numeric / NULLIF(bs.wickets, 0)::numeric,
    2
  ) as bowling_strike_rate,
  round(
    bs.dot_balls::numeric * 100.0 / NULLIF(bs.balls_bowled, 0)::numeric,
    2
  ) as dot_ball_percentage
from
  bowling_spells bs
  left join corr c on c.match_id = bs.match_id
  and c.inning = bs.inning
  and c.bowler_id = bs.bowler_id
  join players p on bs.bowler_id = p.player_id
  left join teams t on bs.batting_team_id = t.team_id
  join seasons s on bs.season_id = s.season_id;



create materialized view public.player_career_stats_mv as
with
  batting_stats as (
    select
      bp.batter_id,
      bp.batter_name,
      bp.innings_type,
      count(distinct bp.match_id) as matches_played,
      count(*) as innings_batted,
      sum(bp.runs_scored) as total_runs,
      sum(bp.balls_faced) as total_balls,
      sum(bp.was_dismissed) as times_dismissed,
      max(bp.runs_scored) as highest_score,
      sum(
        case
          when bp.innings_category = 'century'::text then 1
          else 0
        end
      ) as centuries,
      sum(
        case
          when bp.innings_category = 'fifty'::text then 1
          else 0
        end
      ) as fifties,
      sum(
        case
          when bp.innings_category = 'duck'::text then 1
          else 0
        end
      ) as ducks,
      sum(bp.fours) as total_fours,
      sum(bp.sixes) as total_sixes,
      round(avg(bp.strike_rate), 2) as avg_strike_rate
    from
      batting_performance_mv bp
    group by
      bp.batter_id,
      bp.batter_name,
      bp.innings_type
  ),
  bowling_stats as (
    select
      bo_1.bowler_id,
      bo_1.bowler_name,
      bo_1.innings_type,
      count(distinct bo_1.match_id) as matches_bowled,
      count(*) as innings_bowled,
      sum(bo_1.balls_bowled) as total_balls_bowled,
      sum(bo_1.runs_conceded) as total_runs_conceded,
      sum(bo_1.wickets) as total_wickets,
      max(bo_1.wickets) as best_bowling_figures,
      sum(bo_1.dot_balls) as total_dot_balls
    from
      bowler_performance_mv bo_1
    group by
      bo_1.bowler_id,
      bo_1.bowler_name,
      bo_1.innings_type
  )
select
  COALESCE(b.batter_id, bo.bowler_id) as player_id,
  COALESCE(b.batter_name, bo.bowler_name) as player_name,
  COALESCE(b.innings_type, bo.innings_type) as innings_type,
  b.matches_played,
  b.innings_batted,
  b.total_runs,
  b.total_balls,
  b.times_dismissed,
  b.highest_score,
  b.centuries,
  b.fifties,
  b.ducks,
  b.total_fours,
  b.total_sixes,
  round(
    b.total_runs / NULLIF(b.times_dismissed, 0)::numeric,
    2
  ) as batting_average,
  round(
    b.total_runs * 100.0 / NULLIF(b.total_balls, 0::numeric),
    2
  ) as career_strike_rate,
  round(
    bo.total_runs_conceded::numeric / NULLIF(bo.total_wickets, 0::numeric),
    2
  ) as bowling_average,
  round(
    bo.total_balls_bowled / NULLIF(bo.total_wickets, 0::numeric),
    2
  ) as bowling_strike_rate,
  round(
    bo.total_runs_conceded::numeric * 6.0 / NULLIF(bo.total_balls_bowled, 0::numeric),
    2
  ) as avg_economy_rate,
  bo.matches_bowled,
  bo.innings_bowled,
  bo.total_balls_bowled,
  bo.total_runs_conceded,
  bo.total_wickets,
  bo.best_bowling_figures,
  bo.total_dot_balls
from
  batting_stats b
  full join bowling_stats bo on b.batter_id = bo.bowler_id
  and b.innings_type = bo.innings_type;



create materialized view public.player_innings_mv as
with
  innings_aggregates as (
    select
      d.match_id,
      d.inning,
      d.batter_id,
      d.bowling_team_id,
      m.season_id,
      m.match_date,
      m.match_type,
      m.venue_id,
      m.winner_id,
      m.super_over,
      case
        when d.inning <= 2 then 'regular'::text
        else 'super_over'::text
      end as innings_type,
      sum(COALESCE(d.batsman_runs::integer, 0)) as runs_scored,
      count(
        case
          when d.extras_type is null
          or (
            d.extras_type <> all (
              array[
                'wide'::extras_type_enum,
                'noball'::extras_type_enum
              ]
            )
          ) then 1
          else null::integer
        end
      ) as balls_faced,
      sum(
        case
          when d.batsman_runs = 4 then 1
          else 0
        end
      ) as fours,
      sum(
        case
          when d.batsman_runs = 6 then 1
          else 0
        end
      ) as sixes,
      max(
        case
          when d.is_wicket
          and d.player_dismissed_id = d.batter_id then 1
          else 0
        end
      ) as was_dismissed,
      max(
        case
          when d.is_wicket
          and d.player_dismissed_id = d.batter_id then d.dismissal_kind::text
          else null::text
        end
      ) as dismissal_type,
      sum(
        case
          when COALESCE(d.total_runs::integer, 0) = 0 then 1
          else 0
        end
      ) as dot_balls
    from
      deliveries d
      join matches m on d.match_id = m.match_id
    where
      d.inning <= 4
    group by
      d.match_id,
      d.inning,
      d.batter_id,
      d.bowling_team_id,
      m.season_id,
      m.match_date,
      m.match_type,
      m.venue_id,
      m.winner_id,
      m.super_over
  )
select
  ia.match_id,
  ia.inning,
  ia.batter_id,
  ia.bowling_team_id,
  ia.season_id,
  ia.match_date,
  ia.match_type,
  ia.venue_id,
  ia.winner_id,
  ia.super_over,
  ia.innings_type,
  ia.runs_scored,
  ia.balls_faced,
  ia.fours,
  ia.sixes,
  ia.was_dismissed,
  ia.dismissal_type,
  ia.dot_balls,
  p.player_name,
  t.team_name as opposition_team,
  s.season_year,
  v.venue_name,
  round(
    ia.runs_scored::numeric * 100.0 / NULLIF(ia.balls_faced, 0)::numeric,
    2
  ) as strike_rate,
  case
    when ia.runs_scored >= 100 then 'century'::text
    when ia.runs_scored >= 50 then 'fifty'::text
    when ia.runs_scored = 0
    and ia.was_dismissed = 1 then 'duck'::text
    else 'normal'::text
  end as innings_category
from
  innings_aggregates ia
  join players p on ia.batter_id = p.player_id
  left join teams t on ia.bowling_team_id = t.team_id
  join seasons s on ia.season_id = s.season_id
  join venues v on ia.venue_id = v.venue_id;



create materialized view public.player_vs_team_mv as
select
  'batting'::text as performance_type,
  bp.batter_id as player_id,
  bp.batter_name as player_name,
  bp.bowling_team_id as opponent_team_id,
  bp.bowling_team as opponent_team,
  count(distinct bp.match_id) as matches,
  count(*) as innings,
  sum(bp.runs_scored) as total_runs,
  sum(bp.balls_faced) as total_balls,
  sum(bp.was_dismissed) as times_dismissed,
  max(bp.runs_scored) as highest_score,
  round(avg(bp.runs_scored), 2) as avg_runs,
  round(avg(bp.strike_rate), 2) as avg_strike_rate,
  sum(
    case
      when bp.innings_category = 'century'::text then 1
      else 0
    end
  ) as centuries,
  sum(
    case
      when bp.innings_category = 'fifty'::text then 1
      else 0
    end
  ) as fifties
from
  batting_performance_mv bp
where
  bp.innings_type = 'regular'::text
group by
  bp.batter_id,
  bp.batter_name,
  bp.bowling_team_id,
  bp.bowling_team
union all
select
  'bowling'::text as performance_type,
  bo.bowler_id as player_id,
  bo.bowler_name as player_name,
  bo.batting_team_id as opponent_team_id,
  bo.batting_team as opponent_team,
  count(distinct bo.match_id) as matches,
  count(*) as innings,
  sum(bo.runs_conceded) as total_runs,
  sum(bo.balls_bowled) as total_balls,
  sum(bo.wickets) as times_dismissed,
  max(bo.wickets) as highest_score,
  round(
    sum(bo.runs_conceded)::numeric * 1.0 / NULLIF(sum(bo.wickets), 0::numeric),
    2
  ) as avg_runs,
  round(
    sum(bo.balls_bowled) * 1.0 / NULLIF(sum(bo.wickets), 0::numeric),
    2
  ) as avg_strike_rate,
  sum(
    case
      when bo.wickets >= 5 then 1
      else 0
    end
  ) as centuries,
  sum(
    case
      when bo.wickets >= 3 then 1
      else 0
    end
  ) as fifties
from
  bowler_performance_mv bo
where
  bo.innings_type = 'regular'::text
group by
  bo.bowler_id,
  bo.bowler_name,
  bo.batting_team_id,
  bo.batting_team;



create materialized view public.season_batting_stats_mv as
select
  bp.batter_id,
  bp.batter_name,
  bp.season_year,
  count(distinct bp.match_id) as matches,
  count(*) as innings,
  sum(bp.runs_scored) as total_runs,
  sum(bp.balls_faced) as total_balls,
  sum(bp.was_dismissed) as times_dismissed,
  max(bp.runs_scored) as highest_score,
  round(
    sum(bp.runs_scored) * 1.0 / NULLIF(sum(bp.was_dismissed), 0)::numeric,
    2
  ) as batting_average,
  round(
    sum(bp.runs_scored) * 100.0 / NULLIF(sum(bp.balls_faced), 0::numeric),
    2
  ) as strike_rate,
  sum(
    case
      when bp.innings_category = 'century'::text then 1
      else 0
    end
  ) as centuries,
  sum(
    case
      when bp.innings_category = 'fifty'::text then 1
      else 0
    end
  ) as fifties,
  sum(bp.fours) as fours,
  sum(bp.sixes) as sixes
from
  batting_performance_mv bp
where
  bp.innings_type = 'regular'::text
group by
  bp.batter_id,
  bp.batter_name,
  bp.season_year;


create materialized view public.season_bowling_stats_mv as
select
  bo.bowler_id,
  bo.bowler_name,
  bo.season_year,
  count(distinct bo.match_id) as matches,
  count(*) as innings,
  sum(bo.balls_bowled) as total_balls,
  sum(bo.runs_conceded) as total_runs,
  sum(bo.wickets) as total_wickets,
  round(
    sum(bo.runs_conceded)::numeric * 6.0 / NULLIF(sum(bo.balls_bowled), 0::numeric),
    2
  ) as economy_rate,
  round(
    sum(bo.runs_conceded)::numeric * 1.0 / NULLIF(sum(bo.wickets), 0::numeric),
    2
  ) as bowling_average,
  round(
    sum(bo.balls_bowled) * 1.0 / NULLIF(sum(bo.wickets), 0::numeric),
    2
  ) as strike_rate,
  sum(bo.dot_balls) as dot_balls,
  max(bo.wickets) as best_figures
from
  bowler_performance_mv bo
where
  bo.innings_type = 'regular'::text
group by
  bo.bowler_id,
  bo.bowler_name,
  bo.season_year;