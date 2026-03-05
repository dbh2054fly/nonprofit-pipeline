CREATE TABLE IF NOT EXISTS foundations (
    ein VARCHAR,
    business_name VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zipcode VARCHAR,
    url VARCHAR,
    mission VARCHAR,
    does_grants BOOLEAN,
    assets DOUBLE,
    revenue DOUBLE,
    expenses DOUBLE,
    grants_paid DOUBLE,
    return_type VARCHAR,
    UNIQUE (ein, return_type)
);

CREATE TABLE IF NOT EXISTS grants (
    ein VARCHAR,
    recipient VARCHAR,
    amount DOUBLE,
    purpose VARCHAR,
    UNIQUE (ein, recipient, amount)
);