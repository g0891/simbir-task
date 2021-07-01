package org.noname.statistics;

import java.util.Optional;

public interface StatisticsDAO {
    boolean isCreated();
    Optional<Statistics> get(String url);
    void create(Statistics statistics);
    void update(Statistics statistics);
    void delete(String url);
    void close();

}
