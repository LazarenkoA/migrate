create table IF NOT EXISTS AgentChannels(
Channel varchar(255) not null,
AgentID varchar(255) not null,
Field varchar(255),
PRIMARY KEY (Channel, AgentID))