import model.*;

import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        DataSource dataSource = new DataSource();

        if (!dataSource.open()) {
            System.out.println("Cannot open the data source. Please try later!");
            return;
        }


        List<String> songs = dataSource.queryAlbumsForArtist("Deep Purple");
        songs.sort(String::compareToIgnoreCase);
        for (String song : songs){
            System.out.println(song);
        }
        System.out.println("<><><><><><><>><>");
        List<SongArtist> songArtists = dataSource.queryArtistsForSong("Heartless");
        if (songArtists != null) {
            for (SongArtist songArtist : songArtists) {
                System.out.println(songArtist.getArtistName() + " | " + songArtist.getAlbumName() + " | " + songArtist.getTrack());
            }
        }
//        Scanner scanner = new Scanner(System.in);
//
//        dataSource.getMetaDataForTable("songs");
//        System.out.println(dataSource.getCount("songs"));
//        System.out.println(dataSource.getCount(DataSource.TABLE_ARTIST));
//
//        System.out.println(dataSource.viewForArtistAlbumSongs());
//
//        List<SongArtist> songArtists1 = dataSource.queryViewBySongName(scanner.nextLine());
//        for (SongArtist songArtist : songArtists1){
//            System.out.println(songArtist.getArtistName() + " | " + songArtist.getAlbumName() + " | " + songArtist.getTrack());
//        }
//        System.out.println("{}{}{}{}{}{}");
//        System.out.println("Enter the name of the song: ");
//        String songTitle = scanner.nextLine();
//        List<SongArtist> songArtists2 = dataSource.queryViewWithPreparedStatement(songTitle);
//        for (SongArtist songArtist : songArtists2){
//            System.out.println(songArtist.getArtistName() + ": " + songArtist.getAlbumName()+": " + songArtist.getTrack());
//        }
//        List<Artist> artists = dataSource.queryArtists();
//        List<Album> albums = dataSource.queryAlbums();
//        List<Song> songs = dataSource.querySongs();


//        for (Artist artist : artists) {
//            if (artist.getName().equals("Deep Purple")) {
//                System.out.println("ID: " + artist.getId() + "| Name: " + artist.getName());
//                for (Album album : albums) {
//                    if (album.getArtistId() == artist.getId()) {
//                        System.out.println("Album: " + album.getName());
//                        for (Song song : songs) {
//                            if (album.getId() == Integer.parseInt(song.getAlbumId())){
//                                System.out.println(song.getTrack() + "| " + song.getTitle());
//                            }
//                        }
//                    }
//                }
//            }
//        }

        dataSource.insertSong("Bird Dog","Everly Brothers", "All-Time Greatest Hits", 5);
        dataSource.close();
    }
}
