/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.api;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Additional methods added to classes. These must be static methods with receiver as first argument */
public class Augmentation {
    
    // static methods only!
    private Augmentation() {}

    /** Exposes List.size() as getLength(), so that .length shortcut works on lists */
    public static <T> int getLength(List<T> receiver) {
        return receiver.size();
    }

    /** Exposes Matcher.group(String) as namedGroup(String), so it doesn't conflict with group(int) */
    public static String namedGroup(Matcher receiver, String name) {
        return receiver.group(name);
    }
    
    // some groovy methods on iterable
    // see http://docs.groovy-lang.org/latest/html/groovy-jdk/java/lang/Iterable.html
    
    /** Iterates over the contents of an iterable, and checks whether a predicate is valid for at least one element. */
    public static <T> boolean any(Iterable<T> receiver, Predicate<T> predicate) {
        for (T t : receiver) {
            if (predicate.test(t)) {
                return true;
            }
        }
        return false;
    }
    
    /** Converts this Iterable to a Collection. Returns the original Iterable if it is already a Collection. */
    public static <T> Collection<T> asCollection(Iterable<T> receiver) {
        if (receiver instanceof Collection) {
            return (Collection<T>)receiver;
        }
        List<T> list = new ArrayList<>();
        for (T t : receiver) {
            list.add(t);
        }
        return list;
    }
    
    /** Converts this Iterable to a List. Returns the original Iterable if it is already a List. */
    public static <T> List<T> asList(Iterable<T> receiver) {
        if (receiver instanceof List) {
            return (List<T>)receiver;
        }
        List<T> list = new ArrayList<>();
        for (T t : receiver) {
            list.add(t);
        }
        return list;
    }
    
    /** Counts the number of occurrences which satisfy the given predicate from inside this Iterable. */ 
    public static <T> int count(Iterable<T> receiver, Predicate<T> predicate) {
        int count = 0;
        for (T t : receiver) {
            if (predicate.test(t)) {
                count++;
            }
        }
        return count;
    }
    
    // instead of covariant overrides for every possibility, we just return receiver as 'def' for now
    // that way if someone chains the calls, everything works.

    /** Iterates through an Iterable, passing each item to the given consumer. */
    public static <T> Object each(Iterable<T> receiver, Consumer<T> consumer) {
        receiver.forEach(consumer);
        return receiver;
    }
    
    /** 
     * Iterates through an iterable type, passing each item and the item's index 
     * (a counter starting at zero) to the given consumer.
     */
    public static <T> Object eachWithIndex(Iterable<T> receiver, ObjIntConsumer<T> consumer) {
        int count = 0;
        for (T t : receiver) {
            consumer.accept(t, count++);
        }
        return receiver;
    }
    
    /**
     * Used to determine if the given predicate is valid (i.e. returns true for all items in this iterable).
     */
    public static <T> boolean every(Iterable<T> receiver, Predicate<T> predicate) {
        for (T t : receiver) {
            if (predicate.test(t) == false) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Iterates through the Iterable transforming items using the supplied function and 
     * collecting any non-null results. 
     */
    public static <T,U> List<U> findResults(Iterable<T> receiver, Function<T,U> filter) {
        List<U> list = new ArrayList<>();
        for (T t: receiver) {
           U result = filter.apply(t);
           if (result != null) {
               list.add(result);
           }
        }
        return list;
    }
    
    /**
     * Sorts all Iterable members into groups determined by the supplied mapping function. 
     */
    public static <T,U> Map<U,List<T>> groupBy(Iterable<T> receiver, Function<T,U> mapper) {
        Map<U,List<T>> map = new LinkedHashMap<>();
        for (T t : receiver) {
            U mapped = mapper.apply(t);
            List<T> results = map.get(mapped);
            if (results == null) {
                results = new ArrayList<>();
                map.put(mapped, results);
            }
            results.add(t);
        }
        return map;
    }
    
    /**
     * Concatenates the toString() representation of each item in this Iterable, 
     * with the given String as a separator between each item. 
     */
    public static <T> String join(Iterable<T> receiver, String separator) {
        StringBuilder sb = new StringBuilder();
        for (T t : receiver) {
            if (sb.length() > 0) {
                sb.append(separator);
            }
            sb.append(t);
        }
        return sb.toString();
    }
    
    /**
     * Sums the result of an Iterable
     */
    public static <T extends Number> double sum(Iterable<T> receiver) {
        double sum = 0;
        for (T t : receiver) {
            sum += t.doubleValue();
        }
        return sum;
    }
    
    /**
     * Sums the result of applying a function to each item of an Iterable. 
     */
    public static <T> double sum(Iterable<T> receiver, ToDoubleFunction<T> function) {
        double sum = 0;
        for (T t : receiver) {
            sum += function.applyAsDouble(t);
        }
        return sum;
    }
    
    // some groovy methods on collection
    // see http://docs.groovy-lang.org/latest/html/groovy-jdk/java/util/Collection.html
    
    /**
     * Iterates through this collection transforming each entry into a new value using 
     * the function, returning a list of transformed values. 
     */
    public static <T,U> List<U> collect(Collection<T> receiver, Function<T,U> function) {
        List<U> list = new ArrayList<>();
        for (T t : receiver) {
            list.add(function.apply(t));
        }
        return list;
    }
    
    /**
     * Iterates through this collection transforming each entry into a new value using 
     * the function, adding the values to the specified collection.
     */
    public static <T,U> Object collect(Collection<T> receiver, Collection<U> collection, Function<T,U> function) {
        for (T t : receiver) {
            collection.add(function.apply(t));
        }
        return collection;
    }
    
    /**
     * Finds the first value matching the predicate, or returns null.
     */
    public static <T> T find(Collection<T> receiver, Predicate<T> predicate) {
        for (T t : receiver) {
            if (predicate.test(t)) {
                return t;
            }
        }
        return null;
    }
    
    /**
     * Finds all values matching the predicate, returns as a list
     */
    public static <T> List<T> findAll(Collection<T> receiver, Predicate<T> predicate) {
        List<T> list = new ArrayList<>();
        for (T t : receiver) {
            if (predicate.test(t)) {
                list.add(t);
            }
        }
        return list;
    }
    
    /**
     * Iterates through the collection calling the given function for each item 
     * but stopping once the first non-null result is found and returning that result. 
     * If all results are null, null is returned. 
     */
    public static <T,U> Object findResult(Collection<T> receiver, Function<T,U> function) {
        return findResult(receiver, null, function);
    }
    
    /**
     * Iterates through the collection calling the given function for each item 
     * but stopping once the first non-null result is found and returning that result. 
     * If all results are null, defaultResult is returned.
     */
    public static <T,U> Object findResult(Collection<T> receiver, Object defaultResult, Function<T,U> function) {
        for (T t : receiver) {
            U value = function.apply(t);
            if (value != null) {
                return value;
            }
        }
        return defaultResult;
    }
    
    /**
     * Splits all items into two collections based on the predicate. 
     * The first list contains all items which match the closure expression. The second list all those that don't. 
     */
    public static <T> List<List<T>> split(Collection<T> receiver, Predicate<T> predicate) {
        List<T> matched = new ArrayList<>();
        List<T> unmatched = new ArrayList<>();
        List<List<T>> result = new ArrayList<>(2);
        result.add(matched);
        result.add(unmatched);
        for (T t : receiver) {
            if (predicate.test(t)) {
                matched.add(t);
            } else {
                unmatched.add(t);
            }
        }
        return result;
    }
    
    // some groovy methods on map
    // see http://docs.groovy-lang.org/latest/html/groovy-jdk/java/util/Map.html
    
    /**
     * Iterates through this map transforming each entry into a new value using 
     * the function, returning a list of transformed values. 
     */
    public static <K,V,T> List<T> collect(Map<K,V> receiver, BiFunction<K,V,T> function) {
        List<T> list = new ArrayList<>();
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
            list.add(function.apply(kvPair.getKey(), kvPair.getValue()));
        }
        return list;
    }
    
    /**
     * Iterates through this map transforming each entry into a new value using 
     * the function, adding the values to the specified collection.
     */
    public static <K,V,T> Object collect(Map<K,V> receiver, Collection<T> collection, BiFunction<K,V,T> function) {
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
            collection.add(function.apply(kvPair.getKey(), kvPair.getValue()));
        }
        return collection;
    }
    
    /** Counts the number of occurrences which satisfy the given predicate from inside this Map */ 
    public static <K,V> int count(Map<K,V> receiver, BiPredicate<K,V> predicate) {
        int count = 0;
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                count++;
            }
        }
        return count;
    }
    
    /** Iterates through a Map, passing each item to the given consumer. */
    public static <K,V> Object each(Map<K,V> receiver, BiConsumer<K,V> consumer) {
        receiver.forEach(consumer);
        return receiver;
    }
    
    /**
     * Used to determine if the given predicate is valid (i.e. returns true for all items in this map).
     */
    public static <K,V> boolean every(Map<K,V> receiver, BiPredicate<K,V> predicate) {
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue()) == false) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Finds the first entry matching the predicate, or returns null.
     */
    public static <K,V> Map.Entry<K,V> find(Map<K,V> receiver, BiPredicate<K,V> predicate) {
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                return kvPair;
            }
        }
        return null;
    }
    
    /**
     * Finds all values matching the predicate, returns as a map.
     */
    public static <K,V> Map<K,V> findAll(Map<K,V> receiver, BiPredicate<K,V> predicate) {
        // try to preserve some properties of the receiver (see the groovy javadocs)
        final Map<K,V> map;
        if (receiver instanceof TreeMap) {
            map = new TreeMap<>();
        } else {
            map = new LinkedHashMap<>();
        }
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
            if (predicate.test(kvPair.getKey(), kvPair.getValue())) {
                map.put(kvPair.getKey(), kvPair.getValue());
            }
        }
        return map;
    }
    
    /**
     * Iterates through the map calling the given function for each item 
     * but stopping once the first non-null result is found and returning that result. 
     * If all results are null, null is returned. 
     */
    public static <K,V,T> Object findResult(Map<K,V> receiver, BiFunction<K,V,T> function) {
        return findResult(receiver, null, function);
    }
    
    /**
     * Iterates through the map calling the given function for each item 
     * but stopping once the first non-null result is found and returning that result. 
     * If all results are null, defaultResult is returned.
     */
    public static <K,V,T> Object findResult(Map<K,V> receiver, Object defaultResult, BiFunction<K,V,T> function) {
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
            T value = function.apply(kvPair.getKey(), kvPair.getValue());
            if (value != null) {
                return value;
            }
        }
        return defaultResult;
    }
    
    /**
     * Iterates through the map transforming items using the supplied function and 
     * collecting any non-null results. 
     */
    public static <K,V,T> List<T> findResults(Map<K,V> receiver, BiFunction<K,V,T> filter) {
        List<T> list = new ArrayList<>();
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
           T result = filter.apply(kvPair.getKey(), kvPair.getValue());
           if (result != null) {
               list.add(result);
           }
        }
        return list;
    }
    
    /**
     * Sorts all Map members into groups determined by the supplied mapping function. 
     */
    public static <K,V,T> Map<T,Map<K,V>> groupBy(Map<K,V> receiver, BiFunction<K,V,T> mapper) {
        Map<T,Map<K,V>> map = new LinkedHashMap<>();
        for (Map.Entry<K,V> kvPair : receiver.entrySet()) {
            T mapped = mapper.apply(kvPair.getKey(), kvPair.getValue());
            Map<K,V> results = map.get(mapped);
            if (results == null) {
                // try to preserve some properties of the receiver (see the groovy javadocs)
                if (receiver instanceof TreeMap) {
                    results = new TreeMap<>();
                } else {
                    results = new LinkedHashMap<>();
                }
                map.put(mapped, results);
            }
            results.put(kvPair.getKey(), kvPair.getValue());
        }
        return map;
    }

    // CharSequence augmentation
    /**
     * Replace all matches. Similar to {@link Matcher#replaceAll(String)} but allows you to customize the replacement based on the match.
     */
    public static String replaceAll(CharSequence receiver, Pattern pattern, Function<Matcher, String> replacementBuilder) {
        Matcher m = pattern.matcher(receiver);
        if (false == m.find()) {
            // CharSequqence's toString is *supposed* to always return the characters in the sequence as a String
            return receiver.toString();
        }
        StringBuffer result = new StringBuffer(initialBufferForReplaceWith(receiver));
        do {
            m.appendReplacement(result, Matcher.quoteReplacement(replacementBuilder.apply(m)));
        } while (m.find());
        m.appendTail(result);
        return result.toString();
    }

    /**
     * Replace the first match. Similar to {@link Matcher#replaceFirst(String)} but allows you to customize the replacement based on the
     * match.
     */
    public static String replaceFirst(CharSequence receiver, Pattern pattern, Function<Matcher, String> replacementBuilder) {
        Matcher m = pattern.matcher(receiver);
        if (false == m.find()) {
            // CharSequqence's toString is *supposed* to always return the characters in the sequence as a String
            return receiver.toString();
        }
        StringBuffer result = new StringBuffer(initialBufferForReplaceWith(receiver));
        m.appendReplacement(result, Matcher.quoteReplacement(replacementBuilder.apply(m)));
        m.appendTail(result);
        return result.toString();
    }

    /**
     * The initial size of the {@link StringBuilder} used for {@link #replaceFirst(CharSequence, Pattern, Function)} and
     * {@link #replaceAll(CharSequence, Pattern, Function)} for a particular sequence. We ape
     * {{@link StringBuilder#StringBuilder(CharSequence)} here and add 16 extra chars to the buffer to have a little room for growth.
     */
    private static int initialBufferForReplaceWith(CharSequence seq) {
        return seq.length() + 16;
    }

    /**
     * Encode a String in Base64. Use {@link Base64.Encoder#encodeToString(byte[])} if you have to encode bytes rather than a string.
     */
    public static String encodeBase64(String receiver) {
        return Base64.getEncoder().encodeToString(receiver.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decode some Base64 bytes and build a UTF-8 encoded string. Use {@link Base64.Decoder#decode(String)} if you'd prefer bytes to work
     * with bytes.
     */
    public static String decodeBase64(String receiver) {
        return new String(Base64.getDecoder().decode(receiver.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }
}
