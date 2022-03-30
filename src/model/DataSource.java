package model;

import com.sun.source.tree.WhileLoopTree;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource {
    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:/Users/uktamnimatov/IdeaProjects/MusicDB/" + DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public static final String TABLE_ARTIST = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTIST + " (" + COLUMN_ARTIST_NAME + ") values(?)";
    public static final String INSERT_ALBUM = "INSERT INTO " + TABLE_ALBUMS + " (" + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST +
            ") values(?, ?)";
    public static final String INSERT_SONGS = "INSERT INTO " + TABLE_SONGS + " (" + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " +
            COLUMN_SONG_ALBUM + ") values(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " + TABLE_ARTIST + " WHERE " +
            COLUMN_ARTIST_NAME + " = ?";
    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " + TABLE_ALBUMS + " WHERE " +
            COLUMN_ALBUM_NAME + " = ?";


    public static Connection conn;

    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;

    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            insertIntoArtists = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = conn.prepareStatement(INSERT_ALBUM, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = conn.prepareStatement(INSERT_SONGS);
            queryArtist = conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUM);

            System.out.println("Successfully connected to the database");
            return true;
        } catch (SQLException e) {
            System.out.println("Could not connect to the database " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public void close() {
        try {
            if (insertIntoSongs != null) {
                insertIntoSongs.close();
            }
            if (insertIntoArtists != null) {
                insertIntoArtists.close();
            }
            if (insertIntoAlbums != null) {
                insertIntoAlbums.close();
            }
            if (queryArtist != null) {
                queryArtist.close();
            }
            if (queryAlbum != null) {
                queryAlbum.close();
            }
            conn.close();
            System.out.println("Successfully unconnected from the database");
        } catch (SQLException e) {
            System.out.println("Could not close the connection " + e.getMessage());
            e.printStackTrace();
        }
    }

    public List<Artist> queryArtists() {//using try-with-resources in this method

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery("SELECT * FROM " + TABLE_ARTIST)) {

            List<Artist> artists = new ArrayList<>();
            while (results.next()) {
                Artist artist = new Artist();
                artist.setId(results.getInt(INDEX_ARTIST_ID));
                artist.setName(results.getString(INDEX_ARTIST_NAME));
                artists.add(artist);
            }
            return artists;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return null;
        }
    }


    public List<Album> queryAlbums() {

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery("SELECT * FROM " + TABLE_ALBUMS)) {

            List<Album> albums = new ArrayList<>();
            while (results.next()) {
                Album album = new Album();
                album.setId(results.getInt(INDEX_ALBUM_ID));
                album.setName(results.getString(INDEX_ALBUM_NAME));
                album.setArtistId(results.getInt(INDEX_ALBUM_ARTIST));

                albums.add(album);
            }
            return albums;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return null;
        }
    }

    public List<Song> querySongs() {

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery("SELECT * FROM " + TABLE_SONGS)) {

            List<Song> songs = new ArrayList<>();
            while (results.next()) {
                Song song = new Song();
                song.setId(results.getInt(INDEX_SONG_ID));
                song.setTrack(results.getInt(INDEX_SONG_TRACK));
                song.setTitle(results.getString(INDEX_SONG_TITLE));
                song.setAlbumId(results.getString(INDEX_SONG_ALBUM));

                songs.add(song);
            }
            return songs;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return null;
        }
    }

    public List<String> queryAlbumsForArtist(String artistName) {

        List<String> songs = new ArrayList<>();
        String sb = "SELECT " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS + " INNER JOIN " +
                TABLE_ARTIST + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
                "=" + TABLE_ARTIST + "." + COLUMN_ARTIST_ID +
                " WHERE " + TABLE_ARTIST + "." + COLUMN_ALBUM_NAME + "=" + "'" + artistName + "'" +
                " ORDER BY " + TABLE_ALBUMS + "." + COLUMN_ARTIST_NAME + ";";
        System.out.println(sb);
        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb)) {
            while (results.next()) {
                songs.add(results.getString(1));
            }
            return songs;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public List<SongArtist> queryArtistsForSong(String songTitle) {
        List<SongArtist> songArtists = new ArrayList<>();
        String query = "SELECT " + TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME +
                ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK + " FROM " + TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " +
                TABLE_SONGS + "." + COLUMN_SONG_ALBUM + "=" + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID + " INNER JOIN " + TABLE_ARTIST +
                " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + "=" + TABLE_ARTIST + "." + COLUMN_ARTIST_ID + " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE + "="
                + "'" + songTitle + "'" + " ORDER BY " +
                TABLE_ARTIST + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK + ";";
        System.out.println(query);
        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumName(results.getString(2));
                songArtist.setTrack(results.getInt(3));
                songArtists.add(songArtist);
            }
            return songArtists;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public void getMetaDataForTable(String tableName) {
        String query = "SELECT * FROM " + tableName;
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            System.out.println("There " + resultSetMetaData.getColumnCount() + " columns in the table");
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                System.out.println(resultSetMetaData.getColumnName(i) + " of type " + resultSetMetaData.getColumnTypeName(i));
            }
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
        }
    }

    /*SELECT COUNT(*) FROM SONGS ==>*/
    public int getCount(String tableName) {
        String query = "SELECT COUNT(*) AS count FROM " + tableName;
        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(query)) {
            return results.getInt("count");
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return -1;
        }
    }

    public boolean viewForArtistAlbumSongs() {
        String query = "CREATE VIEW IF NOT EXISTS artist_album_songs AS SELECT artists.name AS artist, albums.name " +
                "AS album, songs.title AS title, songs.track AS track FROM songs INNER JOIN albums ON songs.album=albums._id " +
                "INNER JOIN artists ON albums.artist=artists._id ORDER BY artists.name, albums.name, songs.track";

        try (Statement statement = conn.createStatement()) {
            statement.execute(query);
            return true;
        } catch (SQLException e) {
            System.out.println("Creating view failed " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public List<SongArtist> queryViewBySongName(String title) {
        String query = "SELECT artist, album, track FROM artist_album_songs WHERE title = " + "\"" + title + "\"";
        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(query)) {

            List<SongArtist> songArtists = new ArrayList<>();
            while (results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString("artist"));
                songArtist.setAlbumName(results.getString("album"));
                songArtist.setTrack(results.getInt("track"));

                songArtists.add(songArtist);
            }
            return songArtists;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    public List<SongArtist> queryViewWithPreparedStatement(String title) {
        String query = "SELECT artist, album, track FROM artist_album_songs WHERE title" + " = ?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setString(1, title);
            ResultSet results = preparedStatement.executeQuery();

            List<SongArtist> songArtists = new ArrayList<>();
            while (results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString("artist"));
                songArtist.setAlbumName(results.getString("album"));
                songArtist.setTrack(results.getInt("track"));

                songArtists.add(songArtist);
            }

            return songArtists;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private int insertArtist(String name) throws SQLException {
        queryArtist.setString(1, name);
        ResultSet result = queryArtist.executeQuery();
        if (result.next()) {
            return result.getInt(1);
        }

        try {
            insertIntoArtists.setString(1, name);
            int affectedRows = insertIntoArtists.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("Could not insert artist");
            }

            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Could not get _id for the artist");
            }
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }

    private int insertAlbum(String album, int artistId) throws SQLException {
        queryAlbum.setString(1, album);
//        queryAlbum.setInt(2, artistId);
         ResultSet result = queryAlbum.executeQuery();
        if (result.next()) {
            return result.getInt(1);
        }

        try {
            insertIntoAlbums.setString(1, album);
            insertIntoAlbums.setInt(2, artistId);

            int affectedRows = insertIntoAlbums.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("Could not insert artist");
            }

            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Could not get _id for the album");
            }
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }

    public void insertSong(String title, String artist, String album, int track) {

        try {
            conn.setAutoCommit(false);

            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);

            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(3, albumId);

            int affectedRows = insertIntoSongs.executeUpdate();

            if (affectedRows == 1) {
                conn.commit();
            }else {
                throw new SQLException("Could not insert artist");
            }
        } catch (SQLException e) {
            System.out.println("Insert song exception " + e.getMessage());
            try{
                System.out.println("Performing rollback");
                conn.rollback();
            }catch (SQLException e2){
                System.out.println("Oh boy! Things are really bad! " + e2.getMessage());
            }
        }finally {
            try{
                System.out.println("Resetting default commit behavoir");
                conn.setAutoCommit(true);
            }catch (SQLException e){
                System.out.println("Could not reset auto commit " + e.getMessage());
            }
        }
    }
}











