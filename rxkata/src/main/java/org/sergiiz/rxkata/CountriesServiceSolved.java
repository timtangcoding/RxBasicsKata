package org.sergiiz.rxkata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.Single;
import io.reactivex.functions.BiFunction;
import io.reactivex.schedulers.Schedulers;
import javafx.collections.ObservableList;

class CountriesServiceSolved implements CountriesService {

    @Override
    public Single<String> countryNameInCapitals(Country country) {
        return Single.just(country.getName().toUpperCase());
    }

    public Single<Integer> countCountries(List<Country> countries) {
        return Single.just(countries.size());
    }

    public Observable<Long> listPopulationOfEachCountry(List<Country> countries) {
        return Observable.fromIterable(countries).map(new io.reactivex.functions.Function<Country, Long>() {
            @Override
            public Long apply(Country country) throws Exception {
                return country.population;
            }
        });
    }

    @Override
    public Observable<String> listNameOfEachCountry(List<Country> countries) {
        return Observable.fromIterable(countries).map(new io.reactivex.functions.Function<Country, String>() {
            @Override
            public String apply(Country country) throws Exception {
                return country.name;
            }
        });
    }

    @Override
    public Observable<Country> listOnly3rdAnd4thCountry(List<Country> countries) {

        return Observable.fromIterable(countries).skip(2).take(2);
    }

    @Override
    public Single<Boolean> isAllCountriesPopulationMoreThanOneMillion(List<Country> countries) {
        return  Observable.fromIterable(countries).all(new io.reactivex.functions.Predicate<Country>() {
            @Override
            public boolean test(Country country) throws Exception {
                return country.population > 1000000;
            }
        });
    }

    @Override
    public Observable<Country> listPopulationMoreThanOneMillion(List<Country> countries) {

        return Observable.fromIterable(countries).filter(new io.reactivex.functions.Predicate<Country>() {
            @Override
            public boolean test(Country country) throws Exception {
                return country.population > 1000000;
            }
        });
    }

    @Override
    public Observable<Country> listPopulationMoreThanOneMillionWithTimeoutFallbackToEmpty(final FutureTask<List<Country>> countriesFromNetwork) {
        return Observable.fromFuture(countriesFromNetwork, Schedulers.io())
        .flatMap(new io.reactivex.functions.Function<List<Country>, ObservableSource<Country>>() {
            @Override
            public ObservableSource<Country> apply(List<Country> countries) throws Exception {
                return Observable.fromIterable(countries);
            }
        })
        .filter(new io.reactivex.functions.Predicate<Country>() {

            @Override
            public boolean test(Country country) throws Exception {
                return country.population > 1000000;

            }
        }).timeout(1, TimeUnit.SECONDS, Observable.empty());

    }



    @Override
    public Observable<String> getCurrencyUsdIfNotFound(String countryName, List<Country> countries) {

        return Observable.fromIterable(countries)
                .filter(new io.reactivex.functions.Predicate<Country>() {
                    @Override
                    public boolean test(Country country) throws Exception {
                         return country.name.equalsIgnoreCase(countryName);
                    }
                }).map(new io.reactivex.functions.Function<Country, String>() {
                    @Override
                    public String apply(Country country) throws Exception {
                        return country.getCurrency();
                    }
                }).switchIfEmpty(Observable.just("USD"));

    }

    @Override
    public Observable<Long> sumPopulationOfCountries(List<Country> countries) {

        return Observable.fromIterable(countries)
                .map(new io.reactivex.functions.Function<Country, Long>() {
                    @Override
                    public Long apply(Country country) throws Exception {
                        return country.getPopulation();
                    }
                })
                .reduce(new BiFunction<Long, Long, Long>() {
                    @Override
                    public Long apply(Long aLong, Long aLong2) throws Exception {
                        return aLong + aLong2;
                    }
                }).toObservable();
    }

    @Override
    public Single<Map<String, Long>> mapCountriesToNamePopulation(List<Country> countries) {
        return Observable.fromIterable(countries)
                .toMap(new io.reactivex.functions.Function<Country, String>() {
                    @Override
                    public String apply(Country country) throws Exception {
                        return country.name;
                    }
                }).map(new io.reactivex.functions.Function<Map<String, Country>, Map<String, Long>>() {
                    @Override
                    public Map<String, Long> apply(Map<String, Country> stringCountryMap) throws Exception {
                        Map<String, Long> returnMap = new HashMap<String, Long>();
                        for(Map.Entry<String, Country> entry : stringCountryMap.entrySet()){
                            returnMap.put(entry.getKey(), entry.getValue().population);
                        }

                        return returnMap;
                    }
                });
    }

    @Override
    public Observable<Long> sumPopulationOfCountries(Observable<Country> countryObservable1,
                                                     Observable<Country> countryObservable2) {
        return Observable.merge(countryObservable1, countryObservable2)
                .map(new io.reactivex.functions.Function<Country, Long>() {
                    @Override
                    public Long apply(Country country) throws Exception {
                        return country.population;
                    }
                })
                .reduce(new BiFunction<Long, Long, Long>() {
                    @Override
                    public Long apply(Long population1, Long sum) throws Exception {
                        return sum + population1;
                    }
                }).toObservable();
    }

    @Override
    public Single<Boolean> areEmittingSameSequences(Observable<Country> countryObservable1,
                                                    Observable<Country> countryObservable2) {
        return Observable.sequenceEqual(countryObservable1, countryObservable2);
    }
}
