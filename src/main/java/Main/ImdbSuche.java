package Main;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.sql.*;

/*
Maven Dependency
<dependency>
        <groupId>com.beust</groupId>
        <artifactId>jcommander</artifactId>
        <version>1.72</version>
</dependency>
 */

public class ImdbSuche {
    private final String SQL_dropView = "DROP VIEW IF EXISTS actormw";
    private final String SQL_createView = "CREATE VIEW actormw as ((select * from actor) union all (select * from actress)) ";
    private final String SQL_getNamesLike = "SELECT DISTINCT name FROM actormw WHERE name LIKE ? ORDER BY name ASC";
    private final String SQL_getTitlesLike = "SELECT DISTINCT title, mid, year FROM movie WHERE title LIKE ? ORDER BY title ASC";
    private final String SQL_getActors = "SELECT DISTINCT name FROM (actormw INNER JOIN movie ON (mid=movie_id AND mid=?)) AS actors FETCH FIRST 5 ROWS ONLY";
    private final String SQL_getGenres = "SELECT DISTINCT genre FROM (genre INNER JOIN movie ON (mid=movie_id AND mid=?)) AS genres";
    private final String SQL_getMovies = "SELECT DISTINCT title FROM (actormw INNER JOIN movie ON (mid=movie_id AND name=?)) AS actors";
    private final String SQL_getCoStars = "SELECT DISTINCT actor2.name, count(*) as count FROM actormw as actor1, actormw as actor2 WHERE actor1.name != actor2.name AND actor1.name=? and actor1.movie_id=actor2.movie_id GROUP BY actor2.name ORDER BY count DESC, name ASC  FETCH FIRST 5 ROWS ONLY";
    @Parameter(names = "-d", required = true)
    private String database;
    @Parameter(names = "-s", required = true)
    private String ip;
    @Parameter(names = "-p", required = true)
    private int port;
    @Parameter(names = "-u", required = true)
    private String username;
    @Parameter(names = "-pw", required = true)
    private String password;
    @Parameter(names = "-k", required = true)
    private String keyword;
    @Parameter(names = "--driver", required = false, hidden = true)
    private String JDBC_DRIVER = "org.postgresql.Driver";

    public static void main(String[] args) {
        ImdbSuche imdbSuche = new ImdbSuche();
        JCommander.newBuilder().addObject(imdbSuche).args(args).build();
        imdbSuche.run();
    }

    public void run() {
        String URL = String.format("jdbc:postgresql://%s:%d/%s", ip, port, database);
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            Connection db = DriverManager.getConnection(URL, username, password);

            PreparedStatement dropView = db.prepareStatement(SQL_dropView);
            dropView.execute();
            dropView.close();

            PreparedStatement createView = db.prepareStatement(SQL_createView);
            createView.execute();
            createView.close();

            PreparedStatement statementTitles = db.prepareStatement(SQL_getTitlesLike);
            PreparedStatement statementActors = db.prepareStatement(SQL_getActors);
            PreparedStatement statementGenres = db.prepareStatement(SQL_getGenres);
            statementTitles.setString(1, "%" + keyword + "%");
            System.out.println("MOVIES");
            ResultSet rsTitles = statementTitles.executeQuery();
            ResultSet rsActors;
            ResultSet rsGenres;
            while (rsTitles.next()) {
                System.out.printf("\t%s, %s", rsTitles.getString(1), rsTitles.getString(3));
                statementGenres.setString(1, rsTitles.getString(2));
                rsGenres = statementGenres.executeQuery();
                while (rsGenres.next()) {
                    System.out.printf(", %s", rsGenres.getString(1));
                }
                System.out.print("\n");
                rsGenres.close();
                statementActors.setString(1, rsTitles.getString(2));
                rsActors = statementActors.executeQuery();
                while (rsActors.next()) {
                    System.out.printf("\t\t%s\n", rsActors.getString(1));
                }
                rsActors.close();
                System.out.println();
            }
            statementGenres.close();
            statementActors.close();
            statementTitles.close();
            rsTitles.close();

            System.out.println();

            PreparedStatement statementNames = db.prepareStatement(SQL_getNamesLike);
            PreparedStatement statementPlayedIn = db.prepareStatement(SQL_getMovies);
            PreparedStatement statementCoStars = db.prepareStatement(SQL_getCoStars);
            statementNames.setString(1, "%" + keyword + "%");

            System.out.println("ACTORS");
            ResultSet rsNames = statementNames.executeQuery();
            ResultSet rsPlayedIn;
            ResultSet rsCoStars;
            while (rsNames.next()) {
                System.out.printf("\t%s\n", rsNames.getString(1));
                System.out.println("\tPLAYED IN:");
                statementPlayedIn.setString(1, rsNames.getString(1));
                rsPlayedIn = statementPlayedIn.executeQuery();
                while (rsPlayedIn.next()) {
                    System.out.printf("\t\t%s\n", rsPlayedIn.getString(1));
                }
                rsPlayedIn.close();
                System.out.println("\tCO-STARS:");
                statementCoStars.setString(1, rsNames.getString(1));
                rsCoStars = statementCoStars.executeQuery();
                while (rsCoStars.next()) {
                    System.out.printf("\t\t%s(%d)\n", rsCoStars.getString(1), rsCoStars.getInt(2));
                }
                rsCoStars.close();
                System.out.println();
            }
            statementCoStars.close();
            statementPlayedIn.close();
            statementNames.close();
            rsNames.close();
            db.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
