package org.noname.statistics;

import org.noname.Main;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;

public class DBStatisticsDAO implements StatisticsDAO{
    //final String DRIVER = "org.h2.Driver";
    final String connectionPath = "jdbc:h2:./db/testDB";
    private Connection conn;
    private Statement stmt;
    private PreparedStatement getUrlsCountStatement;
    private PreparedStatement getUrlsStatisticsStatement;
    private PreparedStatement deleteStatisticsForUrlStatement;
    private PreparedStatement deleteUrlStatement;
    private PreparedStatement insertUrlStatement;
    private PreparedStatement insertWordStatement;
    private PreparedStatement insertWordStatisticsStatement;

    private boolean created = false;

    public DBStatisticsDAO () {
        try {
            Main.logger.log(Level.INFO, "Connecting to DB...");
            conn = DriverManager.getConnection(connectionPath);
            conn.setAutoCommit(true);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS URLS ("
                    + "ID INT PRIMARY KEY AUTO_INCREMENT,"
                    + "URL VARCHAR UNIQUE NOT NULL"
                    + ")");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS WORDS ("
                    + "ID INT PRIMARY KEY AUTO_INCREMENT,"
                    + "WORD VARCHAR UNIQUE NOT NULL"
                    + ")");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS STATISTICS ("
                    + "URL_ID INT NOT NULL,"
                    + "WORD_ID INT NOT NULL,"
                    + "COUNT INT NOT NULL,"
                    + "CONSTRAINT fk_url FOREIGN KEY (URL_ID) REFERENCES URLS(ID),"
                    + "CONSTRAINT fk_word FOREIGN KEY (WORD_ID) REFERENCES WORDS(ID),"
                    + "CONSTRAINT pk_statistics PRIMARY KEY (URL_ID, WORD_ID)"
                    + ")");
            getUrlsCountStatement = conn.prepareStatement("SELECT * FROM URLS WHERE URL=?");
            getUrlsStatisticsStatement = conn.prepareStatement("select words.word as WORD, stat.count as COUNT " +
                    "from (select ID from URLS where URL=? limit 1) url " +
                    "LEFT JOIN Statistics stat ON url.ID=stat.URL_ID " +
                    "LEFT JOIN words on stat.WORD_ID=words.ID");
            deleteStatisticsForUrlStatement = conn.prepareStatement("delete from STATISTICS WHERE URL_ID in (select ID from URLS WHERE URL=?)");
            deleteUrlStatement = conn.prepareStatement("delete from URLS where URL=?");
            insertUrlStatement = conn.prepareStatement("insert into URLS (URL) values(?)");
            insertWordStatement = conn.prepareStatement("insert into WORDS (word) SELECT ? WHERE NOT EXISTS (SELECT word FROM words where word=?)");
            insertWordStatisticsStatement = conn.prepareStatement("insert into statistics values((select ID from URLS where url=?),(select id from words where word=?),?)");

            created = true;
            Main.logger.log(Level.INFO, "Connected to DB.");
        } catch (SQLException ex) {
            Main.logger.log(Level.SEVERE, "Failed to connect to DB.", ex);
        }
    }

    @Override
    public boolean isCreated() {
        return created;
    }


    @Override
    public Optional<Statistics> get(String url) {
        try {

            getUrlsCountStatement.setString(1, url);
            ResultSet rs = getUrlsCountStatement.executeQuery();
            if (!rs.next()) return Optional.empty();
            getUrlsStatisticsStatement.setString(1, url);
            rs = getUrlsStatisticsStatement.executeQuery();
            Map<String,Integer> wordsMap = new HashMap<>();

            while (rs.next()) {
                wordsMap.put(rs.getString("WORD"),rs.getInt("COUNT"));
            }

            return Optional.of(new Statistics(url, wordsMap));

        } catch (SQLException ex) {
            Main.logger.log(Level.SEVERE,"Failed to get Statistics from DB.", ex);
            return Optional.empty();
        }
    }

    @Override
    public void create(Statistics statistics) {
        try {
            insertUrlStatement.setString(1, statistics.getUrl());
            insertUrlStatement.executeUpdate();
            for(String word: statistics.getWordsMap().keySet()) {
                insertWordStatement.setString(1, word);
                insertWordStatement.setString(2, word);
                insertWordStatement.executeUpdate();
            }
            insertWordStatisticsStatement.setString(1, statistics.getUrl());
            for (Map.Entry<String,Integer> entry: statistics.getWordsMap().entrySet()) {
                insertWordStatisticsStatement.setString(2, entry.getKey());
                insertWordStatisticsStatement.setInt(3, entry.getValue());
                insertWordStatisticsStatement.executeUpdate();
            }
        } catch (SQLException ex) {
            Main.logger.log(Level.SEVERE,"Failed to save statistics in DB.", ex);
        }
    }

    @Override
    public void update(Statistics statistics) {
        try {
            deleteStatisticsForUrlStatement.setString(1, statistics.getUrl());
            deleteStatisticsForUrlStatement.executeUpdate();
            deleteUrlStatement.setString(1, statistics.getUrl());
            deleteUrlStatement.executeUpdate();
            create(statistics);
        } catch (SQLException ex) {
            Main.logger.log(Level.SEVERE,"Failed to save statistics in DB.", ex);
        }
    }

    @Override
    public void delete(String url) {
        try {
            deleteStatisticsForUrlStatement.setString(1, url);
            deleteStatisticsForUrlStatement.executeUpdate();
            deleteUrlStatement.setString(1, url);
            deleteUrlStatement.executeUpdate(url);
        } catch (SQLException ex) {
            Main.logger.log(Level.SEVERE,"Failed to delete Statistics from DB.", ex);
        }
    }

    @Override
    public void close() {
        if (created) {
            try {
                stmt.close();
                getUrlsCountStatement.close();
                getUrlsStatisticsStatement.close();
                deleteStatisticsForUrlStatement.close();
                deleteUrlStatement.close();
                insertUrlStatement.close();
                insertWordStatement.close();
                insertWordStatisticsStatement.close();

            } catch (SQLException ex) {
                Main.logger.log(Level.SEVERE, "Failed to close DB connection correctly.", ex);
            }
        }
    }
}
