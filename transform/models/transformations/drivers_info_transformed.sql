WITH cleaned_data AS (
    SELECT
        circuit_name,
        year,
        case
            when session_type = 'r' then 'race'
        end as session_type,
        DriverNumber,
        BroadcastName,
        Abbreviation,
        DriverId,
        TeamName,
        TeamColor,
        TRY_TO_NUMBER(TeamId) AS team_id,
        FirstName,
        LastName,
        CONCAT(FirstName, ' ', LastName) AS full_name, -- Combine first and last name
        HeadshotUrl,
        CountryCode,
        TRY_TO_NUMBER(Position) AS position,
        ClassifiedPosition,
        GridPosition,
        Q1,
        Q2,
        Q3,
        Time,
        Status,
        TRY_TO_NUMBER(Points) AS points,
        LOAD_TIME
    FROM {{ ref('drivers_info_staging') }} -- Refers to f1_dashboard.public.drivers_info_staging
)

SELECT
    circuit_name,
    year,
    session_type,
    DriverNumber,
    BroadcastName,
    Abbreviation,
    DriverId,
    TeamName,
    TeamColor,
    team_id,
    full_name,
    HeadshotUrl,
    CountryCode,
    position,
    ClassifiedPosition,
    GridPosition,
    Q1,
    Q2,
    Q3,
    Time,
    Status,
    points,
    LOAD_TIME
FROM cleaned_data;