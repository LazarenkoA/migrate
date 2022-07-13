create table IF NOT EXISTS Agents (AgentID varchar(255) not null primary key,
Status varchar(255) not null, Reason varchar(255) not null, LastReadyTimeAt timestamp not null)