package rx;

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.observables.ConnectableObservable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class RxCombineObservablesReproducer {

    public static void main(String[] args) {
        RxCombineObservablesReproducer reproducer = new RxCombineObservablesReproducer();
        //this will work as indented
        reproducer.combineLatest();
        //this not
        reproducer.combineLatestToFast();
    }

    private void combineLatest() {

        ConnectableObservable<Data> priceObs = Observable.interval(100, TimeUnit.MILLISECONDS).map(i -> new Data("price", i * 1.00, Instant.now())).publish();
        Observable<Data> indicator1Obs = priceObs.map(i -> new Data("indicator1", i.getValue() * 2.00, i.getTs())).delay(10, TimeUnit.MILLISECONDS);
        Observable<Data> indicator2Obs = priceObs.map(i -> new Data("indicator2", i.getValue() / 2.0, i.getTs())).delay(50, TimeUnit.MILLISECONDS);

        Disposable dis = Observable.combineLatest(priceObs, indicator1Obs, indicator2Obs, (Data price, Data indicator1, Data indicator2) -> {
                    if (checkSameTs(price, indicator1, indicator2)) {
                        Map result = new HashMap<String, Data>();
                        result.put("price", price);
                        result.put("indicator1", indicator1);
                        result.put("indicator2", indicator2);
                        return result;
                    }
                    return new HashMap<String, Data>();
                }

        )
                .filter(m -> !m.isEmpty())
                .subscribe(System.out::println);
        priceObs.connect();
        sleep(1000);
        dis.dispose();
    }

    private void combineLatestToFast() {

        ConnectableObservable<Data> priceObsToFast = Observable.interval(100, TimeUnit.MILLISECONDS).map(i -> new Data("price", i * 1.00, Instant.now())).publish();
        Observable<Data> indicator1ObsToFast = priceObsToFast.map(i -> new Data("indicator1ToFast", i.getValue() * 2.00, i.getTs())).delay(9, TimeUnit.MILLISECONDS);
        Observable<Data> indicator2ObsToFast = priceObsToFast.map(i -> new Data("indicator2ToFast", i.getValue() / 2.00, i.getTs())).delay(15, TimeUnit.MILLISECONDS);

        System.out.println("\n------------------------------toFast------------------------------\n");
        Disposable dis = Observable.combineLatest(priceObsToFast, indicator1ObsToFast, indicator2ObsToFast, (Data priceToFast, Data indicator1ToFast, Data indicator2ToFast) -> {
                    //System.out.println("combineToFast");
                    if (checkSameTs(priceToFast, indicator1ToFast, indicator2ToFast)) {
                        Map result = new HashMap<String, Data>();
                        result.put("priceToFast", priceToFast);
                        result.put("indicator1ToFast", indicator1ToFast);
                        result.put("indicator2ToFast", indicator2ToFast);
                        return result;
                    }
                    return new HashMap<String, Data>();
                }

        )
                .filter(m -> !m.isEmpty())
                .subscribe(System.out::println);
        priceObsToFast.connect();
        sleep(1000);
        dis.dispose();
    }

    private boolean checkSameTs(Data... dataObjects) {
        Instant ts = dataObjects[0].getTs();
        boolean same = true;
        for (Data data : dataObjects) {
            if (!data.getTs().equals(ts)) {
                same = false;
                break;
            }
        }
        return same;
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class Data {
        private String name;
        private Double value;
        private Instant ts;

        Data(String name, Double value, Instant ts) {
            this.name = name;
            this.value = value;
            this.ts = ts;
        }

        public String getName() {
            return name;
        }

        Double getValue() {
            return value;
        }

        Instant getTs() {
            return ts;
        }

        @Override
        public String toString() {
            return "Data{" +
                    "ts=" + ts +
                    ",name='" + name + '\'' +
                    ", value=" + value +
                    '}';
        }
    }


}
