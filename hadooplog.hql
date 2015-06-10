create table if not exists loginfo(
    rdate string,
    time array<string>,
    type string,
    relateclass string,
    information1 string,
    information2 string,
    information3 string)
row format delimited fields terminated by ' '
collection items terminated by ','  
map keys terminated by  ':';