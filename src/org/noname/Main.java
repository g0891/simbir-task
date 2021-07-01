package org.noname;

import java.io.IOException;
import java.util.Optional;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import org.noname.statistics.*;

public class Main {

    public static final Logger logger;
    public static final Scanner in = new Scanner(System.in);
    public static StatisticsDAO statisticsStorage;

    static {
        try {
            LogManager.getLogManager().readConfiguration(Main.class.getClassLoader().getResourceAsStream("org/noname/log.properties"));
        } catch (IOException ex) {
            System.out.println("Failed to load properties for logging.");
        }

        logger = Logger.getLogger(Main.class.getName());
        logger.log(Level.INFO, "Application started.");

        statisticsStorage = new DBStatisticsDAO();
        //statisticsStorage = new MapStatisticsDAO();
    }

    public static void main(String[] args) {

        if (!statisticsStorage.isCreated()) {
            logger.log(Level.INFO, "Application stopped.");
            return;
        }

        boolean terminate = false;
        String url;
        Optional<Statistics> savedStatistics;

        while (!terminate) {
            System.out.println("\nPlease select:\n[1] Get statistics for URL\n[2] Exit");
            switch (in.nextLine()) {
                case "1":
                    System.out.print("\nPlease enter URL: ");
                    url = in.nextLine();
                    logger.log(Level.INFO, "Requested URL: " + url);
                    savedStatistics = statisticsStorage.get(url);
                    if (savedStatistics.isPresent()) {
                        System.out.println("Do you want to get previously calculated statistics for this URL or calculate a new one?");
                        System.out.println("[1] Get previously calculated\n[2] Calculate a new one");
                        switch (in.nextLine()) {
                            case "1":
                                savedStatistics.get().print();
                                break;
                            case "2":
                                makeNewStatistics(url);
                                break;
                            default:
                                System.out.println("Incorrect input. 1 or 2 expected.");
                        }
                    } else {
                        makeNewStatistics(url);
                    }
                    break;
                case "2":
                    terminate = true;
                    break;
                default:
                    System.out.println("Incorrect input. 1 or 2 expected.");
            }
        }

        statisticsStorage.close();
        logger.log(Level.INFO, "Application stopped.");


    }

    private static void makeNewStatistics(String url) {
        try {
            Document doc = Jsoup.connect(url).get();
            logger.log(Level.INFO, "Page downloaded.");
            Statistics statistics = new Statistics(url, doc.text());
            statistics.print();
            statisticsStorage.update(statistics);
        } catch (Exception ex) {
            System.out.println("Failed to calculate new statistics.");
            logger.log(Level.SEVERE, "Failed to download page.", ex);
        }
    }

}
