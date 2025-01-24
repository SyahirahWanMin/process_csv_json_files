-- Table: Users_tbl
CREATE TABLE users_tbl (
    user_id UUID PRIMARY KEY,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    logged_at TIMESTAMP,
    name VARCHAR,
    dob DATE,
    address VARCHAR,
    username VARCHAR,
    password VARCHAR,
    national_id VARCHAR,
    car_brand VARCHAR,
    car_license_plate VARCHAR,
    is_active BOOLEAN,
    inserted_date TIMESTAMP
);

-- Table: Jobs_History
CREATE TABLE jobs_history (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES Users_tbl(user_id) ON DELETE CASCADE,
    occupation VARCHAR,
    is_fulltime BOOLEAN,
    start DATE,
    end DATE,
    employer VARCHAR,
    inserted_date TIMESTAMP
);

-- Table: Telephone_Numbers
CREATE TABLE telephone_numbers (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES Users_tbl(user_id) ON DELETE CASCADE,
    telephone_number BIGINT,
    inserted_date TIMESTAMP
);

CREATE TABLE testest (
    uuid UUID PRIMARY KEY,
    id INT,
    name VARCHAR,
    address VARCHAR,
    color VARCHAR,
    created_at_ori VARCHAR,
    created_at TIMESTAMP,
    last_login_ori VARCHAR,
    last_login TIMESTAMP,
    is_claimed_ori VARCHAR,
    is_claimed BOOLEAN,
    paid_amount DECIMAL(10,2),
    inserted_date TIMESTAMP
);
