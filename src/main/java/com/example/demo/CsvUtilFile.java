package com.example.demo;
import java.util.List;

public class CsvUtilFile {

        public PlayerRepository playerRepository;

        public CsvUtilFile(PlayerRepository playerRepository) {
            this.playerRepository=playerRepository;
        }

        public List<Player> getPlayers(){
            List<Player> list=playerRepository.findAll();
            return list;
        }

        private CsvUtilFile(){}

}





/*
plantilla anterior
public static List<Player> getPlayers(){
        var uri =  CsvUtilFile.class.getClassLoader().getResource("data.csv");
        List<Player> list = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(uri.getFile()))) {
            List<String[]> registers = reader.readAll();
            registers.forEach(strings -> list.add(new Player(
                    Integer.parseInt(strings[0].trim()),
                    strings[1],
                    Integer.parseInt(Optional.of(strings[2].trim()).filter(h -> !h.isBlank()).orElse("0")),
                    strings[3],
                    strings[4],
                    Integer.parseInt(strings[5].trim()),
                    Integer.parseInt(strings[6].trim()),
                    strings[7]
            )));

           return list;

        } catch (IOException | CsvException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }*/