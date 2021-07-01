package org.noname.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MapStatisticsDAO implements StatisticsDAO {
    Map<String, Statistics>  map = new HashMap<>();

    @Override
    public boolean isCreated() {
        return true;
    }

    @Override
    public Optional<Statistics> get(String url) {
        return Optional.ofNullable(map.get(url));
    }

    @Override
    public void create(Statistics statistics) {
        map.put(statistics.getUrl(), statistics);
    }

    @Override
    public void update(Statistics statistics) {
        create(statistics);
    }

    @Override
    public void delete(String url) {
        map.remove(url);
    }

    @Override
    public void close() {
        //Nothing to close
    }
}
