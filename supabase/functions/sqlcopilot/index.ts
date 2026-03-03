// supabase/functions/sqlcopilot/index.ts
// Ultimate IPL Analytics SQL Copilot - Advanced Intelligence Engine
import { Pool } from "https://deno.land/x/postgres@v0.17.2/mod.ts";
// =====================================================
// CONFIGURATION
// =====================================================
const DB_URL = Deno.env.get('DATABASE_URL') || Deno.env.get('DB_URL');
const GEMINI_API_KEY = Deno.env.get('GEMINI_API_KEY');
const pool = DB_URL ? new Pool(DB_URL, 3, true) : null;
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
};
// =====================================================
// ADVANCED CRICKET INTELLIGENCE
// =====================================================
const CRICKET_INTELLIGENCE = {
  // Performance Benchmarks by Context
  BENCHMARKS: {
    STRIKE_RATE: {
      powerplay: {
        excellent: 140,
        good: 125,
        average: 110,
        poor: 95
      },
      middle_overs: {
        excellent: 150,
        good: 135,
        average: 120,
        poor: 105
      },
      death_overs: {
        excellent: 175,
        good: 160,
        average: 145,
        poor: 130
      },
      overall: {
        excellent: 145,
        good: 130,
        average: 120,
        poor: 110
      }
    },
    ECONOMY_RATE: {
      powerplay: {
        excellent: 6.0,
        good: 7.0,
        average: 8.0,
        poor: 9.5
      },
      middle_overs: {
        excellent: 6.5,
        good: 7.5,
        average: 8.5,
        poor: 9.5
      },
      death_overs: {
        excellent: 8.5,
        good: 10.0,
        average: 11.5,
        poor: 13.0
      },
      overall: {
        excellent: 7.0,
        good: 8.0,
        average: 9.0,
        poor: 10.0
      }
    },
    BATTING_AVERAGE: {
      overall: {
        excellent: 35,
        good: 28,
        average: 22,
        poor: 18
      },
      top_order: {
        excellent: 40,
        good: 32,
        average: 25,
        poor: 20
      },
      middle_order: {
        excellent: 32,
        good: 26,
        average: 20,
        poor: 16
      },
      finisher: {
        excellent: 28,
        good: 22,
        average: 18,
        poor: 14
      }
    },
    BOWLING_AVERAGE: {
      overall: {
        excellent: 20,
        good: 25,
        average: 30,
        poor: 35
      },
      powerplay_specialist: {
        excellent: 22,
        good: 27,
        average: 32,
        poor: 37
      },
      death_specialist: {
        excellent: 24,
        good: 28,
        average: 33,
        poor: 38
      }
    }
  },
  // Statistical Significance Thresholds
  MIN_SAMPLES: {
    BATTING: {
      innings: 10,
      balls: 120,
      runs: 200
    },
    BOWLING: {
      innings: 8,
      balls: 120,
      overs: 20,
      wickets: 10
    },
    PHASE_ANALYSIS: {
      balls: 60
    },
    SEASON_ANALYSIS: {
      matches: 7
    },
    CAREER_ANALYSIS: {
      seasons: 2,
      matches: 15
    },
    VENUE_ANALYSIS: {
      matches: 5
    },
    H2H_ANALYSIS: {
      encounters: 3
    }
  },
  // Match Phase Definitions
  PHASES: {
    POWERPLAY: {
      start: 1,
      end: 6,
      description: "Powerplay (Overs 1-6)"
    },
    MIDDLE: {
      start: 7,
      end: 15,
      description: "Middle overs (7-15)"
    },
    DEATH: {
      start: 16,
      end: 20,
      description: "Death overs (16-20)"
    },
    EARLY_MIDDLE: {
      start: 7,
      end: 10,
      description: "Early middle (7-10)"
    },
    LATE_MIDDLE: {
      start: 11,
      end: 15,
      description: "Late middle (11-15)"
    }
  },
  // Advanced Metrics Definitions
  ADVANCED_METRICS: {
    BATTING: [
      'balls_per_boundary',
      'dot_ball_percentage',
      'boundary_percentage',
      'acceleration_rate',
      'pressure_index',
      'consistency_score',
      'momentum_contribution',
      'anchor_index'
    ],
    BOWLING: [
      'wicket_probability',
      'dot_ball_pressure',
      'boundary_control',
      'breakthrough_index',
      'death_over_specialist_score',
      'powerplay_specialist_score',
      'economy_variation'
    ]
  }
};
// Enhanced Player Database with All Variations
const PLAYER_INTELLIGENCE = new Map([
  // MS Dhoni variations
  [
    'dhoni',
    {
      name: 'MS Dhoni',
      role: 'finisher',
      style: 'calculated',
      team: 'CSK'
    }
  ],
  [
    'msd',
    {
      name: 'MS Dhoni',
      role: 'finisher',
      style: 'calculated',
      team: 'CSK'
    }
  ],
  [
    'mahi',
    {
      name: 'MS Dhoni',
      role: 'finisher',
      style: 'calculated',
      team: 'CSK'
    }
  ],
  [
    'captain cool',
    {
      name: 'MS Dhoni',
      role: 'finisher',
      style: 'pressure_expert',
      team: 'CSK'
    }
  ],
  [
    'thala',
    {
      name: 'MS Dhoni',
      role: 'finisher',
      style: 'death_specialist',
      team: 'CSK'
    }
  ],
  [
    'ms dhoni',
    {
      name: 'MS Dhoni',
      role: 'finisher',
      style: 'calculated',
      team: 'CSK'
    }
  ],
  // Virat Kohli variations
  [
    'kohli',
    {
      name: 'Virat Kohli',
      role: 'top_order',
      style: 'accumulator',
      team: 'RCB'
    }
  ],
  [
    'virat',
    {
      name: 'Virat Kohli',
      role: 'top_order',
      style: 'chase_master',
      team: 'RCB'
    }
  ],
  [
    'king kohli',
    {
      name: 'Virat Kohli',
      role: 'top_order',
      style: 'consistent',
      team: 'RCB'
    }
  ],
  [
    'chiku',
    {
      name: 'Virat Kohli',
      role: 'top_order',
      style: 'chase_master',
      team: 'RCB'
    }
  ],
  [
    'vk',
    {
      name: 'Virat Kohli',
      role: 'top_order',
      style: 'consistent',
      team: 'RCB'
    }
  ],
  [
    'virat kohli',
    {
      name: 'Virat Kohli',
      role: 'top_order',
      style: 'chase_master',
      team: 'RCB'
    }
  ],
  // Rohit Sharma variations
  [
    'rohit',
    {
      name: 'Rohit Sharma',
      role: 'opener',
      style: 'explosive',
      team: 'MI'
    }
  ],
  [
    'hitman',
    {
      name: 'Rohit Sharma',
      role: 'opener',
      style: 'explosive',
      team: 'MI'
    }
  ],
  [
    'ro',
    {
      name: 'Rohit Sharma',
      role: 'opener',
      style: 'powerplay_specialist',
      team: 'MI'
    }
  ],
  [
    'rohit sharma',
    {
      name: 'Rohit Sharma',
      role: 'opener',
      style: 'explosive',
      team: 'MI'
    }
  ],
  // AB de Villiers
  [
    'ab',
    {
      name: 'AB de Villiers',
      role: 'middle_order',
      style: '360_degree',
      team: 'RCB'
    }
  ],
  [
    'abd',
    {
      name: 'AB de Villiers',
      role: 'middle_order',
      style: 'innovative',
      team: 'RCB'
    }
  ],
  [
    'mr 360',
    {
      name: 'AB de Villiers',
      role: 'middle_order',
      style: '360_degree',
      team: 'RCB'
    }
  ],
  [
    'ab de villiers',
    {
      name: 'AB de Villiers',
      role: 'middle_order',
      style: '360_degree',
      team: 'RCB'
    }
  ],
  // Jasprit Bumrah
  [
    'bumrah',
    {
      name: 'Jasprit Bumrah',
      role: 'death_bowler',
      style: 'yorker_specialist',
      team: 'MI'
    }
  ],
  [
    'jasprit',
    {
      name: 'Jasprit Bumrah',
      role: 'death_bowler',
      style: 'yorker_specialist',
      team: 'MI'
    }
  ],
  [
    'jasprit bumrah',
    {
      name: 'Jasprit Bumrah',
      role: 'death_bowler',
      style: 'yorker_specialist',
      team: 'MI'
    }
  ],
  // Other key players
  [
    'gayle',
    {
      name: 'Chris Gayle',
      role: 'opener',
      style: 'power_hitter',
      team: 'RCB/PBKS'
    }
  ],
  [
    'warner',
    {
      name: 'David Warner',
      role: 'opener',
      style: 'aggressive',
      team: 'SRH/DC'
    }
  ],
  [
    'russell',
    {
      name: 'Andre Russell',
      role: 'finisher',
      style: 'explosive',
      team: 'KKR'
    }
  ],
  [
    'malinga',
    {
      name: 'Lasith Malinga',
      role: 'death_bowler',
      style: 'sling_arm',
      team: 'MI'
    }
  ],
  [
    'rashid',
    {
      name: 'Rashid Khan',
      role: 'spinner',
      style: 'leg_spin',
      team: 'SRH/GT'
    }
  ],
  [
    'hardik',
    {
      name: 'Hardik Pandya',
      role: 'all_rounder',
      style: 'finisher',
      team: 'MI/GT'
    }
  ],
  [
    'jadeja',
    {
      name: 'Ravindra Jadeja',
      role: 'all_rounder',
      style: 'accurate',
      team: 'CSK'
    }
  ]
]);
// Team Intelligence
const TEAM_INTELLIGENCE = new Map([
  [
    'csk',
    {
      name: 'Chennai Super Kings',
      identity: 'consistency',
      home: 'Chepauk'
    }
  ],
  [
    'chennai',
    {
      name: 'Chennai Super Kings',
      identity: 'consistency',
      home: 'Chepauk'
    }
  ],
  [
    'mi',
    {
      name: 'Mumbai Indians',
      identity: 'champions',
      home: 'Wankhede'
    }
  ],
  [
    'mumbai',
    {
      name: 'Mumbai Indians',
      identity: 'champions',
      home: 'Wankhede'
    }
  ],
  [
    'rcb',
    {
      name: 'Royal Challengers Bangalore',
      identity: 'star_power',
      home: 'Chinnaswamy'
    }
  ],
  [
    'bangalore',
    {
      name: 'Royal Challengers Bangalore',
      identity: 'star_power',
      home: 'Chinnaswamy'
    }
  ],
  [
    'kkr',
    {
      name: 'Kolkata Knight Riders',
      identity: 'mystique',
      home: 'Eden Gardens'
    }
  ],
  [
    'kolkata',
    {
      name: 'Kolkata Knight Riders',
      identity: 'mystique',
      home: 'Eden Gardens'
    }
  ],
  [
    'dc',
    {
      name: 'Delhi Capitals',
      identity: 'young_guns',
      home: 'Kotla'
    }
  ],
  [
    'delhi',
    {
      name: 'Delhi Capitals',
      identity: 'young_guns',
      home: 'Kotla'
    }
  ],
  [
    'srh',
    {
      name: 'Sunrisers Hyderabad',
      identity: 'bowling_first',
      home: 'Hyderabad'
    }
  ],
  [
    'hyderabad',
    {
      name: 'Sunrisers Hyderabad',
      identity: 'bowling_first',
      home: 'Hyderabad'
    }
  ],
  [
    'rr',
    {
      name: 'Rajasthan Royals',
      identity: 'unpredictable',
      home: 'Jaipur'
    }
  ],
  [
    'rajasthan',
    {
      name: 'Rajasthan Royals',
      identity: 'unpredictable',
      home: 'Jaipur'
    }
  ],
  [
    'pbks',
    {
      name: 'Punjab Kings',
      identity: 'aggressive',
      home: 'Mohali'
    }
  ],
  [
    'punjab',
    {
      name: 'Punjab Kings',
      identity: 'aggressive',
      home: 'Mohali'
    }
  ],
  [
    'gt',
    {
      name: 'Gujarat Titans',
      identity: 'new_champions',
      home: 'Ahmedabad'
    }
  ],
  [
    'gujarat',
    {
      name: 'Gujarat Titans',
      identity: 'new_champions',
      home: 'Ahmedabad'
    }
  ],
  [
    'lsg',
    {
      name: 'Lucknow Super Giants',
      identity: 'strategic',
      home: 'Lucknow'
    }
  ],
  [
    'lucknow',
    {
      name: 'Lucknow Super Giants',
      identity: 'strategic',
      home: 'Lucknow'
    }
  ]
]);
// Query complexity patterns
const QUERY_PATTERNS = {
  SIMPLE: {
    patterns: [
      'top',
      'best',
      'highest',
      'lowest',
      'total',
      'count'
    ],
    mvPreference: 'always',
    expectedTime: 100
  },
  MODERATE: {
    patterns: [
      'compare',
      'versus',
      'average',
      'between',
      'trend'
    ],
    mvPreference: 'prefer',
    expectedTime: 500
  },
  COMPLEX: {
    patterns: [
      'phase',
      'progression',
      'correlation',
      'probability',
      'momentum'
    ],
    mvPreference: 'hybrid',
    expectedTime: 2000
  },
  ADVANCED: {
    patterns: [
      'predict',
      'model',
      'optimize',
      'simulate',
      'forecast'
    ],
    mvPreference: 'base_tables',
    expectedTime: 5000
  }
};
// =====================================================
// ENTITY RESOLUTION & FUZZY MATCHING
// =====================================================
function calculateLevenshteinDistance(str1, str2) {
  const matrix = Array(str2.length + 1).fill(null).map(()=>Array(str1.length + 1).fill(null));
  for(let i = 0; i <= str1.length; i++)matrix[0][i] = i;
  for(let j = 0; j <= str2.length; j++)matrix[j][0] = j;
  for(let j = 1; j <= str2.length; j++){
    for(let i = 1; i <= str1.length; i++){
      const indicator = str1[i - 1] === str2[j - 1] ? 0 : 1;
      matrix[j][i] = Math.min(matrix[j][i - 1] + 1, matrix[j - 1][i] + 1, matrix[j - 1][i - 1] + indicator);
    }
  }
  return matrix[str2.length][str1.length];
}
function resolvePlayerName(input: string, entityCache: Set<string>): string {
  const lower = input.toLowerCase().trim();
  
  // Check nickname database first
  const playerInfo = PLAYER_INTELLIGENCE.get(lower);
  if (playerInfo) {
    // Return just last name for SQL matching
    const nameParts = playerInfo.name.split(' ');
    return nameParts[nameParts.length - 1]; // Return last name only
  }
  
  // Check exact match in cache
  const exactMatch = Array.from(entityCache).find(name => 
    name.toLowerCase() === lower
  );
  if (exactMatch) return exactMatch;
  
  // Extract last name from input for searching
  const inputParts = input.trim().split(' ');
  const inputLastName = inputParts[inputParts.length - 1];
  
  // Try last name match first
  const lastNameMatch = Array.from(entityCache).find(name => {
    const nameParts = name.toLowerCase().split(' ');
    const dbLastName = nameParts[nameParts.length - 1];
    return dbLastName === inputLastName.toLowerCase();
  });
  
  if (lastNameMatch) return lastNameMatch;
  
  // If full name provided, return just the last name for ILIKE
  if (inputParts.length > 1) {
    return inputLastName; // Return just last name for SQL
  }
  
  // Fuzzy matching as fallback
  const candidates = Array.from(entityCache).map(name => ({
    name,
    distance: calculateLevenshteinDistance(lower, name.toLowerCase()),
    similarity: 1 - (calculateLevenshteinDistance(lower, name.toLowerCase()) / 
                     Math.max(lower.length, name.length))
  }));
  
  const goodMatches = candidates
    .filter(c => c.similarity > 0.7)
    .sort((a, b) => b.similarity - a.similarity);
  
  if (goodMatches.length > 0) {
    // Return just the last name from the match
    const nameParts = goodMatches[0].name.split(' ');
    return nameParts[nameParts.length - 1];
  }
  
  // Default: if input has multiple words, use last name only
  if (inputParts.length > 1) {
    return inputLastName;
  }
  
  return input; // Return original for ILIKE query
}
function resolveTeamName(input) {
  const lower = input.toLowerCase().trim();
  const teamInfo = TEAM_INTELLIGENCE.get(lower);
  return teamInfo ? teamInfo.name : input;
}
// =====================================================
// GEMINI API INTEGRATION - Flash 1.5 Optimized
// =====================================================
async function callGemini(prompt, maxTokens = 1024) {
  if (!GEMINI_API_KEY) {
    throw new Error('Gemini API key not configured');
  }
  try {
    const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${GEMINI_API_KEY}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        contents: [
          {
            role: 'user',
            parts: [
              {
                text: prompt
              }
            ]
          }
        ],
        generationConfig: {
          temperature: 0.0,
          maxOutputTokens: maxTokens
        }
      })
    });
    const data = await response.json();
    const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
    if (!response.ok || !text) {
      throw new Error(`Gemini API error: ${data?.error?.message || 'No response'}`);
    }
    return text.trim();
  } catch (error) {
    console.error('Gemini API call failed:', error);
    throw error;
  }
}
// =====================================================
// STAGE 1: ADVANCED QUERY ANALYSIS
// =====================================================
async function analyzeQueryComplexity(question) {
  const prompt = `You are an expert IPL Cricket Data Analyst. Analyze this query for complexity and data requirements.

###CRICKET_KNOWLEDGE###
- IPL format: T20 (20 overs), Powerplay (1-6), Middle (7-15), Death (16-20)
- Key roles: Openers, Middle-order, Finishers, Death bowlers, Spinners
- Important metrics: Strike Rate, Economy, Average, Consistency, Impact
- Situational: Chasing vs Defending, Pressure situations, Phase-specific

###ENTITY_RESOLUTION###
CRITICAL: Database stores mostly LAST NAMES only!
When resolving names:
- "MS Dhoni" → search for "Dhoni"
- "Virat Kohli" → search for "Kohli"  
- "Lasith Malinga" → search for "Malinga"
- "AB de Villiers" → search for "Villiers" or "de Villiers"

Common nicknames:
- Dhoni/Mahi/Thala/Captain Cool = search for "Dhoni"
- Kohli/King Kohli/Chiku = search for "Kohli"
- Hitman/Rohit = search for "Sharma"
- ABD/AB/Mr 360 = search for "Villiers"

In JSON response, entities.players should contain the LAST NAME to search for.
Teams: CSK, MI, RCB, KKR, DC, SRH, RR, PBKS, GT, LSG

###QUESTION###
"${question}"

Respond with ONLY this JSON structure:
{
  "query_intent": "simple_aggregate|comparative|temporal_trend|phase_analysis|situational|predictive|complex_analytical",
  "complexity_score": 1-10,
  "entities": {
    "players": ["exact_names"],
    "teams": ["exact_names"],
    "seasons": ["2023", "2024"],
    "venues": ["venue_names"],
    "phases": ["powerplay", "middle", "death"]
  },
  "metrics_required": {
    "primary": ["runs", "wickets", "average"],
    "derived": ["strike_rate", "economy"],
    "advanced": ["momentum", "pressure_index"]
  },
  "data_requirements": {
    "use_mvs": true/false,
    "need_base_tables": true/false,
    "need_ctes": true/false,
    "need_window_functions": true/false
  },
  "filters": {
    "temporal": "season|career|recent|specific_period",
    "situational": ["chasing", "defending", "pressure"],
    "statistical_minimums": {"innings": 10, "balls": 100}
  },
  "query_routing": "simple_mv|hybrid|complex_cte",
  "expected_response_time_ms": 100-5000
}`;
  try {
    const response = await callGemini(prompt, 800);
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Invalid JSON response');
    const analysis = JSON.parse(jsonMatch[0]);
    // Validate and enhance analysis
    if (!analysis.query_intent) analysis.query_intent = 'simple_aggregate';
    if (!analysis.complexity_score) analysis.complexity_score = 3;
    if (!analysis.query_routing) {
      if (analysis.complexity_score <= 3) analysis.query_routing = 'simple_mv';
      else if (analysis.complexity_score <= 6) analysis.query_routing = 'hybrid';
      else analysis.query_routing = 'complex_cte';
    }
    return analysis;
  } catch (error) {
    console.error('Query analysis failed:', error);
    return {
      query_intent: 'simple_aggregate',
      complexity_score: 3,
      entities: {
        players: [],
        teams: [],
        seasons: [],
        venues: [],
        phases: []
      },
      metrics_required: {
        primary: [
          'runs'
        ],
        derived: [],
        advanced: []
      },
      data_requirements: {
        use_mvs: true,
        need_base_tables: false,
        need_ctes: false
      },
      filters: {
        temporal: 'career',
        situational: [],
        statistical_minimums: {
          innings: 5
        }
      },
      query_routing: 'simple_mv',
      expected_response_time_ms: 500
    };
  }
}
// =====================================================
// STAGE 2: INTELLIGENT SQL GENERATION
// =====================================================
async function generateOptimizedSQL(question, analysis) {
  const routingStrategy = getRoutingStrategy(analysis);
  const prompt = `You are a PostgreSQL expert for IPL cricket analytics. Generate optimal SQL based on routing strategy.

###DATABASE_SCHEMA###
${routingStrategy.schemaContext}

###ROUTING_DECISION###
Strategy: ${analysis.query_routing}
Use MVs: ${analysis.data_requirements.use_mvs}
Need CTEs: ${analysis.data_requirements.need_ctes}

###SQL_RULES###
1. ${routingStrategy.primaryRule}
2. Season years are TEXT ('2023' not 2023)
3. IMPORTANT: For player names, use ILIKE '%last_name%' pattern
   - If given "MS Dhoni", search for ILIKE '%Dhoni%'
   - If given "Lasith Malinga", search for ILIKE '%Malinga%'
   - If given single name "Kohli", search for ILIKE '%Kohli%'
   - Database mostly stores last names, so extract and use last name for matching
4. Apply statistical minimums in HAVING clause
5. ${routingStrategy.optimizationHint}

###EXAMPLES###
${routingStrategy.examples}

###QUESTION###
"${question}"

###ANALYSIS###
${JSON.stringify(analysis, null, 2)}

Generate PostgreSQL query following the routing strategy. Return ONLY SQL, no explanation.`;
  try {
    const response = await callGemini(prompt, 1500);
    let sql = response.replace(/```sql\s*/gi, '').replace(/```\s*/gi, '').trim();
    // Validate SQL
    if (!sql.toLowerCase().includes('select')) {
      throw new Error('Invalid SQL generated');
    }
    // Add safety limits if not present
    if (!sql.toLowerCase().includes('limit') && analysis.query_intent === 'simple_aggregate') {
      sql += '\nLIMIT 20';
    }
    return sql;
  } catch (error) {
    console.error('SQL generation failed:', error);
    return getFallbackSQL(analysis);
  }
}
// Routing strategy helper
function getRoutingStrategy(analysis) {
  if (analysis.query_routing === 'simple_mv') {
    return {
      schemaContext: `
MATERIALIZED VIEWS (USE THESE):
- season_batting_stats_mv: batter_name, season_year, total_runs, batting_average, strike_rate, matches, innings
- season_bowling_stats_mv: bowler_name, season_year, total_wickets, economy_rate, bowling_average, matches
- player_career_stats_mv: player_name, total_runs, batting_average, total_wickets, bowling_average
- player_vs_team_mv: player_name, opponent_team, matches, total_runs, avg_runs`,
      primaryRule: 'Use ONLY materialized views for this query',
      optimizationHint: 'Direct SELECT from appropriate MV',
      examples: `
-- Top scorers in a season
SELECT batter_name, total_runs, batting_average, strike_rate
FROM season_batting_stats_mv
WHERE season_year = '2023'
ORDER BY total_runs DESC
LIMIT 10;`
    };
  } else if (analysis.query_routing === 'complex_cte') {
    return {
      schemaContext: `
BASE TABLES:
- deliveries: match_id, inning, over, ball, batter_id, bowler_id, batsman_runs, extras_type, is_wicket
- matches: match_id, season_id, venue_id, team1_id, team2_id, winner_id, match_date
- players: player_id, player_name
- teams: team_id, team_name
- seasons: season_id, season_year`,
      primaryRule: 'Use CTEs for complex multi-step analysis',
      optimizationHint: 'Build CTEs progressively: filter → aggregate → analyze',
      examples: `
-- Phase-wise performance analysis
WITH phase_data AS (
  SELECT d.batter_id, p.player_name,
    CASE 
      WHEN d.over <= 6 THEN 'powerplay'
      WHEN d.over <= 15 THEN 'middle'
      ELSE 'death'
    END AS phase,
    SUM(d.batsman_runs) AS runs,
    COUNT(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wide','noball') THEN 1 END) AS balls
  FROM deliveries d
  JOIN players p ON d.batter_id = p.player_id
  GROUP BY d.batter_id, p.player_name, phase
),
phase_stats AS (
  SELECT player_name, phase, runs, balls,
    ROUND(runs * 100.0 / NULLIF(balls, 0), 2) AS strike_rate
  FROM phase_data
  WHERE balls >= 30
)
SELECT * FROM phase_stats ORDER BY phase, strike_rate DESC;`
    };
  } else {
    return {
      schemaContext: `
HYBRID APPROACH - Use both MVs and base tables:
MVs for aggregates, base tables for specific details
All tables and views available`,
      primaryRule: 'Combine MVs with base tables using joins or subqueries',
      optimizationHint: 'Start with MVs, add base table details as needed',
      examples: `
-- Recent form with career context
WITH recent_matches AS (
  SELECT batter_id, SUM(batsman_runs) AS recent_runs
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE m.match_date > CURRENT_DATE - INTERVAL '30 days'
  GROUP BY batter_id
)
SELECT pc.player_name, pc.batting_average AS career_avg, 
       rm.recent_runs AS last_month_runs
FROM player_career_stats_mv pc
JOIN recent_matches rm ON pc.player_id = rm.batter_id;`
    };
  }
}
// Fallback SQL for error cases
function getFallbackSQL(analysis) {
  if (analysis.entities.players.length > 0) {
    return `
      SELECT player_name, total_runs, batting_average, 
             matches_played AS matches
      FROM player_career_stats_mv
      WHERE player_name ILIKE '%${analysis.entities.players[0]}%'
        AND innings_type = 'regular'
      LIMIT 1`;
  } else if (analysis.entities.seasons.length > 0) {
    return `
      SELECT batter_name, total_runs, batting_average, strike_rate
      FROM season_batting_stats_mv
      WHERE season_year = '${analysis.entities.seasons[0]}'
      ORDER BY total_runs DESC
      LIMIT 10`;
  } else {
    return `
      SELECT batter_name, total_runs, batting_average, strike_rate
      FROM season_batting_stats_mv
      WHERE season_year = '2024'
      ORDER BY total_runs DESC
      LIMIT 10`;
  }
}

async function executeWithFallback(sql: string, playerName?: string): Promise<any> {
  // First attempt with original SQL
  let result = await executeSQL(sql);
  
  // If no results and player name was involved, try variations
  if ((!result.rows || result.rows.length === 0) && playerName) {
    const nameParts = playerName.split(' ');
    
    // Try just last name
    if (nameParts.length > 1) {
      const lastNameSQL = sql.replace(
        new RegExp(`ILIKE '%${playerName}%'`, 'gi'),
        `ILIKE '%${nameParts[nameParts.length - 1]}%'`
      );
      result = await executeSQL(lastNameSQL);
    }
    
    // If still no results, try first name only
    if ((!result.rows || result.rows.length === 0) && nameParts.length > 1) {
      const firstNameSQL = sql.replace(
        new RegExp(`ILIKE '%${playerName}%'`, 'gi'),
        `ILIKE '%${nameParts[0]}%'`
      );
      result = await executeSQL(firstNameSQL);
    }
  }
  
  return result;
}


// =====================================================
// SQL EXECUTION WITH MONITORING
// =====================================================
async function executeSQL(sql, timeoutMs = 10000) {
  if (!pool) {
    return {
      columns: [],
      rows: [],
      error: 'Database connection not configured'
    };
  }
  const startTime = Date.now();
  try {
    const client = await pool.connect();
    try {
      // Set execution parameters
      await client.queryArray(`SET statement_timeout = '${timeoutMs}ms'`);
      await client.queryArray(`SET work_mem = '256MB'`);
      await client.queryArray(`SET enable_seqscan = off`); // Prefer indexes
      // Execute query
      const result = await client.queryObject(sql);
      const executionTime = Date.now() - startTime;
      // Extract columns
      const columns = result.fields?.map((f)=>f.name) || (result.rows[0] ? Object.keys(result.rows[0]) : []);
      // Process rows (handle BigInt)
      const rows = result.rows.map((row)=>Object.fromEntries(Object.entries(row).map(([k, v])=>[
            k,
            typeof v === 'bigint' ? v.toString() : v
          ])));
      return {
        columns,
        rows,
        executionTime,
        rowCount: rows.length
      };
    } finally{
      client.release();
    }
  } catch (error) {
    console.error('SQL execution error:', error);
    const executionTime = Date.now() - startTime;
    // Check if timeout
    if (error.message?.includes('timeout')) {
      return {
        columns: [],
        rows: [],
        error: 'Query timeout - consider simplifying your question',
        executionTime,
        timedOut: true
      };
    }
    return {
      columns: [],
      rows: [],
      error: error.message || 'Query execution failed',
      executionTime
    };
  }
}
// =====================================================
// STAGE 3: INTELLIGENT RESULT ANALYSIS
// =====================================================
async function generateAnalysis(question, analysis, sqlResults) {
  if (!sqlResults.rows || sqlResults.rows.length === 0) {
    return generateNoDataResponse(question, analysis);
  }
  // For simple queries, provide concise response
  if (analysis.complexity_score <= 3) {
    return generateSimpleAnalysis(sqlResults);
  }
  // For complex queries, provide deeper insights
  const prompt = `You are an IPL cricket expert. Provide concise, data-driven analysis.

QUESTION: "${question}"

DATA (${sqlResults.rows.length} records):
${JSON.stringify(sqlResults.rows.slice(0, 10), null, 2)}

CONTEXT:
- Query type: ${analysis.query_intent}
- Metrics: ${analysis.metrics_required.primary.join(', ')}

Provide a brief, insightful analysis:
1. Direct answer to the question
2. Key statistical insight
3. Cricket context (1-2 sentences max)

Keep response under 150 words. Focus on numbers and cricket intelligence.`;
  try {
    const response = await callGemini(prompt, 500);
    return response;
  } catch (error) {
    console.error('Analysis generation failed:', error);
    return generateSimpleAnalysis(sqlResults);
  }
}
// Generate simple analysis without AI
function generateSimpleAnalysis(sqlResults) {
  const topRows = sqlResults.rows.slice(0, 5);
  const columns = sqlResults.columns;
  let analysis = `**Results Summary**\n\n`;
  // Format top results
  topRows.forEach((row, idx)=>{
    const mainStat = columns[0] ? `**${row[columns[0]]}**` : '';
    const stats = columns.slice(1, 4).map((col)=>`${col.replace(/_/g, ' ')}: ${row[col]}`).join(' | ');
    analysis += `${idx + 1}. ${mainStat} - ${stats}\n`;
  });
  if (sqlResults.rows.length > 5) {
    analysis += `\n*Showing top 5 of ${sqlResults.rows.length} results*`;
  }
  return analysis;
}
// Generate helpful no-data response
function generateNoDataResponse(question, analysis) {
  return `**No Data Found**

This could be due to:
- **Player/Team Name**: Try common names or nicknames
- **Season Range**: Data covers IPL 2008-2024
- **Filters Too Specific**: Consider broader criteria

**Suggestions**:
- Try "Virat Kohli stats 2023" or "top scorers IPL 2024"
- Use team abbreviations: CSK, MI, RCB
- Check player spelling: "MS Dhoni" not "Dhoni MS"`;
}
// =====================================================
// MAIN QUERY HANDLER
// =====================================================
async function handleQuery(question) {
  const startTime = Date.now();
  try {
    console.log(`Processing query: ${question}`);
    // Stage 1: Analyze query complexity and requirements
    const analysis = await analyzeQueryComplexity(question);
    console.log(`Query analysis: complexity=${analysis.complexity_score}, routing=${analysis.query_routing}`);
    // Stage 2: Generate optimized SQL based on routing
    const sql = await generateOptimizedSQL(question, analysis);
    console.log(`SQL generated: ${sql.substring(0, 100)}...`);
    // Stage 3: Execute SQL with appropriate timeout
    const timeoutMs = analysis.expected_response_time_ms || 5000;
    const results = await executeSQL(sql, timeoutMs);
    if (results.error) {
      return {
        error: results.error,
        sql,
        suggestion: results.timedOut ? 'Query too complex. Try breaking it into simpler questions.' : 'Query failed. Check player/team names or try simpler criteria.',
        metadata: {
          stage: 'sql_execution',
          executionTime: Date.now() - startTime,
          complexity_score: analysis.complexity_score,
          query_routing: analysis.query_routing
        }
      };
    }
    // Stage 4: Generate analysis
    const answer = await generateAnalysis(question, analysis, results);
    // Prepare response
    return {
      sql,
      columns: results.columns,
      rows: results.rows,
      answer,
      summary: `${analysis.query_intent.replace(/_/g, ' ')} completed successfully`,
      metadata: {
        executionTime: Date.now() - startTime,
        sqlExecutionTime: results.executionTime,
        rowCount: results.rowCount,
        queryType: analysis.query_intent,
        complexity: analysis.complexity_score,
        routing: analysis.query_routing,
        metricsUsed: analysis.metrics_required.primary
      }
    };
  } catch (error) {
    console.error('Query processing error:', error);
    return {
      error: error.message || 'Query processing failed',
      suggestion: 'Try a simpler question or check the query format',
      metadata: {
        stage: 'processing_error',
        executionTime: Date.now() - startTime
      }
    };
  }
}
// =====================================================
// CACHE MANAGEMENT
// =====================================================
let entityCache = {
  players: new Set(),
  teams: new Set(),
  seasons: new Set(),
  venues: new Set(),
  lastUpdated: 0
};
// Simple query result cache
const queryCache = new Map();
const CACHE_TTL = 300000; // 5 minutes
async function loadEntityCache() {
  if (!pool) return;
  try {
    const client = await pool.connect();
    try {
      // Load all entities in parallel
      const [players, teams, seasons, venues] = await Promise.all([
        client.queryObject(`SELECT DISTINCT player_name FROM players WHERE player_name IS NOT NULL`),
        client.queryObject(`SELECT DISTINCT team_name FROM teams WHERE team_name IS NOT NULL`),
        client.queryObject(`SELECT DISTINCT season_year::text FROM seasons WHERE season_year IS NOT NULL`),
        client.queryObject(`SELECT DISTINCT venue_name FROM venues WHERE venue_name IS NOT NULL`)
      ]);
      players.rows.forEach((r)=>entityCache.players.add(r.player_name));
      teams.rows.forEach((r)=>entityCache.teams.add(r.team_name));
      seasons.rows.forEach((r)=>entityCache.seasons.add(r.season_year));
      venues.rows.forEach((r)=>entityCache.venues.add(r.venue_name));
      entityCache.lastUpdated = Date.now();
      console.log(`Entity cache loaded: ${entityCache.players.size} players, ${entityCache.teams.size} teams, ${entityCache.seasons.size} seasons, ${entityCache.venues.size} venues`);
    } finally{
      client.release();
    }
  } catch (error) {
    console.error('Failed to load entity cache:', error);
  }
}
// Cache query results
function getCachedResult(question) {
  const cached = queryCache.get(question.toLowerCase());
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.result;
  }
  queryCache.delete(question.toLowerCase());
  return null;
}
function cacheResult(question, result) {
  // Limit cache size
  if (queryCache.size > 100) {
    const firstKey = queryCache.keys().next().value;
    queryCache.delete(firstKey);
  }
  queryCache.set(question.toLowerCase(), {
    result,
    timestamp: Date.now()
  });
}
// Initialize cache on startup
loadEntityCache();
// =====================================================
// SCHEMA ENDPOINT
// =====================================================
async function handleSchemaRequest() {
  return {
    status: 'operational',
    message: 'Ultimate IPL Analytics SQL Engine - Advanced Intelligence',
    version: '4.0',
    model: 'gemini-1.5-flash',
    architecture: 'intelligent_routing',
    capabilities: {
      query_routing: [
        'Simple queries → Materialized Views (50ms)',
        'Moderate queries → Hybrid approach (500ms)',
        'Complex queries → CTEs with base tables (2-5s)',
        'Advanced analytics → Multi-stage CTEs (5-10s)'
      ],
      statistical_depth: [
        'Basic: Runs, wickets, averages, strike rates',
        'Intermediate: Phase-wise, venue-specific, H2H',
        'Advanced: Momentum, pressure index, correlations',
        'Expert: Predictive models, what-if scenarios'
      ],
      cricket_intelligence: [
        'Nickname resolution (Thala → MS Dhoni)',
        'Team abbreviations (CSK, MI, RCB)',
        'Situational understanding (death overs, powerplay)',
        'Role identification (finisher, death bowler)',
        'Statistical significance enforcement'
      ],
      performance_features: [
        'Query result caching (5 min TTL)',
        'Entity cache for fuzzy matching',
        'Adaptive timeout based on complexity',
        'Fallback strategies for failures',
        'Progressive result loading'
      ]
    },
    sample_queries: [
      // Simple (MVs only)
      'Top 10 run scorers in IPL 2023',
      'Virat Kohli career stats',
      'Best economy rates this season',
      // Moderate (Hybrid)
      'Compare Rohit vs Kohli in powerplays',
      'MS Dhoni performance in finals',
      'Best death bowlers last 3 seasons',
      // Complex (CTEs)
      'Players with 150+ SR in death overs (min 200 balls)',
      'Batting momentum shift analysis by phase',
      'Win probability contribution in close matches',
      // Advanced
      'Correlation between toss and match outcome at Wankhede',
      'Predict player auction value based on recent performance',
      'Optimal batting order for CSK based on phase performances'
    ],
    statistics: {
      entities_tracked: {
        players: entityCache.players.size,
        teams: entityCache.teams.size,
        seasons: entityCache.seasons.size,
        venues: entityCache.venues.size
      },
      cache_status: {
        last_updated: new Date(entityCache.lastUpdated).toISOString(),
        cached_queries: queryCache.size
      },
      performance_benchmarks: {
        simple_query_target: '100ms',
        complex_query_target: '2000ms',
        cache_hit_rate: 'Not tracked in MVP'
      }
    },
    data_coverage: {
      seasons: '2008-2024',
      matches: '1000+',
      deliveries: '200,000+',
      update_frequency: 'After each match'
    }
  };
}
// =====================================================
// REFRESH MATERIALIZED VIEWS ENDPOINT
// =====================================================
async function refreshMaterializedViews() {
  if (!pool) {
    return {
      error: 'Database not configured'
    };
  }
  const startTime = Date.now();
  const results = {
    refreshed: [],
    failed: [],
    totalTime: 0
  };
  // Order matters - base MVs first, then aggregates
  const mvOrder = [
    'batting_performance_mv',
    'bowler_performance_mv',
    'player_innings_mv',
    'player_career_stats_mv',
    'player_vs_team_mv',
    'season_batting_stats_mv',
    'season_bowling_stats_mv'
  ];
  try {
    const client = await pool.connect();
    try {
      for (const mv of mvOrder){
        const mvStart = Date.now();
        try {
          await client.queryArray(`REFRESH MATERIALIZED VIEW public.${mv}`);
          results.refreshed.push({
            name: mv,
            time: Date.now() - mvStart
          });
          console.log(`Refreshed ${mv} in ${Date.now() - mvStart}ms`);
        } catch (error) {
          results.failed.push({
            name: mv,
            error: error.message
          });
          console.error(`Failed to refresh ${mv}:`, error);
        }
      }
      results.totalTime = Date.now() - startTime;
      // Clear caches after refresh
      queryCache.clear();
      await loadEntityCache();
      return {
        success: results.failed.length === 0,
        ...results
      };
    } finally{
      client.release();
    }
  } catch (error) {
    return {
      success: false,
      error: error.message,
      totalTime: Date.now() - startTime
    };
  }
}
// =====================================================
// HTTP REQUEST HANDLER
// =====================================================
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, (_, v)=>typeof v === 'bigint' ? v.toString() : v), {
    status,
    headers: {
      ...corsHeaders,
      'Content-Type': 'application/json'
    }
  });
}
// =====================================================
// MAIN SERVER
// =====================================================
Deno.serve(async (req)=>{
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, {
      headers: corsHeaders
    });
  }
  const url = new URL(req.url);
  // Schema endpoint
  if (url.pathname.endsWith('/schema')) {
    const schemaData = await handleSchemaRequest();
    return jsonResponse(schemaData);
  }
  // Refresh MVs endpoint (protect with auth in production)
  if (url.pathname.endsWith('/refresh') && req.method === 'POST') {
    const refreshResult = await refreshMaterializedViews();
    return jsonResponse(refreshResult);
  }
  // Main query endpoint
  if (req.method === 'POST') {
    try {
      const { question } = await req.json();
      if (!question || typeof question !== 'string' || question.trim().length === 0) {
        return jsonResponse({
          error: 'Please provide a cricket question',
          suggestion: 'Try "top run scorers 2023" or "Virat Kohli vs MI"'
        }, 400);
      }
      const cleanQuestion = question.trim();
      // Check cache first
      const cached = getCachedResult(cleanQuestion);
      if (cached) {
        console.log('Cache hit for:', cleanQuestion);
        return jsonResponse({
          ...cached,
          metadata: {
            ...cached.metadata,
            cached: true
          }
        });
      }
      // Process query
      const result = await handleQuery(cleanQuestion);
      // Cache successful results
      if (!result.error) {
        cacheResult(cleanQuestion, result);
      }
      return jsonResponse(result);
    } catch (error) {
      console.error('Request processing error:', error);
      return jsonResponse({
        error: 'Invalid request format',
        suggestion: 'Send JSON with "question" field'
      }, 400);
    }
  }
  // Method not allowed
  return jsonResponse({
    error: 'Method not allowed',
    endpoints: {
      'POST /': 'Send cricket queries',
      'GET /schema': 'API information',
      'POST /refresh': 'Refresh materialized views'
    }
  }, 405);
});
