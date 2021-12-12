package com.example.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.*;

@DataMongoTest
public class CSVUtilTest {

    @Autowired
    public PlayerRepository playerRepository;

    private CsvUtilFile csvUtilFile;

    @BeforeEach
    void before(){
        this.csvUtilFile= new CsvUtilFile(playerRepository);
    }

    @Test
    void converterData(){
        List<Player> list = csvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void reactive_filtrarJugadoresMayoresA35(){
        List<Player> list = csvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }

    @Test
    void reactive_filtrarJugadoresMayoresA35DeUnClub(){
        List<Player> list = csvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromIterable(list);
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35 && player.getClub().equals("Chelsea"))
                .collectMultimap(Player::getClub);
        assert listFilter.block().size() == 1;
    }

    @Test
    void reactive_filtrarNacionalidadYRanking() {
        List<Player> list = csvUtilFile.getPlayers();

        Flux<Player> listFlux = Flux.fromIterable(list);
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .sort((p, w) -> w.getWinners() - p.getWinners())
                .collectMultimap(Player::getNational);

        listFilter.block().forEach((national, players) -> {
            System.out.println("\n" + national);
            players.stream().forEach(p -> System.out.println(p.name + "- Partidos ganados: " + p.winners));
        });
    }
}


/*
plantilla de un test anterior
@Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = csvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }*/