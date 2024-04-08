/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.index.VersionType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

public final class RandomDocumentPicks {

    private RandomDocumentPicks() {

    }

    /**
     * Returns a random field name. Can be a leaf field name or the
     * path to refer to a field name using the dot notation.
     */
    public static String randomFieldName(Random random) {
        int numLevels = RandomNumbers.randomIntBetween(random, 1, 5);
        StringBuilder fieldName = new StringBuilder();
        for (int i = 0; i < numLevels - 1; i++) {
            if (i > 0) {
                fieldName.append('.');
            }
            fieldName.append(randomString(random));
        }
        if (numLevels > 1) {
            fieldName.append('.');
        }
        fieldName.append(randomLeafFieldName(random));
        return fieldName.toString();
    }

    /**
     * Returns a random leaf field name.
     */
    public static String randomLeafFieldName(Random random) {
        // Never generates a dot:
        return RandomStrings.randomAsciiAlphanumOfLengthBetween(random, 1, 10);
    }

    /**
     * Returns a randomly selected existing field name out of the fields that are contained
     * in the document provided as an argument.  Does not return the _version field unless it is the only
     * field.
     */
    public static String randomExistingFieldName(Random random, IngestDocument ingestDocument) {
        Map<String, Object> source = new TreeMap<>(ingestDocument.getSourceAndMetadata());
        Map.Entry<String, Object> randomEntry = getRandomEntry(random, source.entrySet());
        String key = randomEntry.getKey();
        while (randomEntry.getValue() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) randomEntry.getValue();
            // we have reached an empty map hence the max depth we can reach
            if (map.isEmpty()) {
                break;
            }
            Map<String, Object> treeMap = new TreeMap<>(map);
            randomEntry = RandomPicks.randomFrom(random, treeMap.entrySet());
            key += "." + randomEntry.getKey();
        }
        assert ingestDocument.getFieldValue(key, Object.class) != null;
        return key;
    }

    /**
     * Return a random entry from a set as long as the entry is not _version.  Returns _version only if it is the only entry.
     * Since _verison has special validation, tests should test it explicitly rather than randomly
     */
    static Map.Entry<String, Object> getRandomEntry(Random random, Set<Map.Entry<String, Object>> entrySet) {
        Map.Entry<String, Object> randomEntry = RandomPicks.randomFrom(random, entrySet);
        String key = randomEntry.getKey();
        while (IngestDocument.Metadata.VERSION.getFieldName().equals(key) && entrySet.size() > 1) {
            randomEntry = RandomPicks.randomFrom(random, entrySet);
            key = randomEntry.getKey();
        }
        return randomEntry;
    }

    /**
     * Adds a random non existing field to the provided document and associates it
     * with the provided value. The field will be added at a random position within the document,
     * not necessarily at the top level using a leaf field name.
     */
    public static String addRandomField(Random random, IngestDocument ingestDocument, Object value) {
        String fieldName;
        do {
            fieldName = randomFieldName(random);
        } while (canAddField(fieldName, ingestDocument) == false);
        ingestDocument.setFieldValue(fieldName, value);
        return fieldName;
    }

    /**
     * Checks whether the provided field name can be safely added to the provided document.
     * When the provided field name holds the path using the dot notation, we have to make sure
     * that each node of the tree either doesn't exist or is a map, otherwise new fields cannot be added.
     */
    public static boolean canAddField(String path, IngestDocument ingestDocument) {
        String[] pathElements = path.split("\\.");
        Map<String, Object> innerMap = ingestDocument.getSourceAndMetadata();
        if (pathElements.length > 1) {
            for (int i = 0; i < pathElements.length - 1; i++) {
                Object currentLevel = innerMap.get(pathElements[i]);
                if (currentLevel == null) {
                    return true;
                }
                if (currentLevel instanceof Map == false) {
                    return false;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) currentLevel;
                innerMap = map;
            }
        }
        String leafKey = pathElements[pathElements.length - 1];
        return innerMap.containsKey(leafKey) == false;
    }

    /**
     * Generates a random document and random metadata
     */
    public static IngestDocument randomIngestDocument(Random random) {
        return randomIngestDocument(random, randomSource(random));
    }

    /**
     * Generates a document that holds random metadata and the document provided as a map argument
     */
    public static IngestDocument randomIngestDocument(Random random, Map<String, Object> source) {
        String index = randomString(random);
        String id = randomString(random);
        String routing = null;
        Long version = randomNonNegtiveLong(random);
        VersionType versionType = RandomPicks.randomFrom(
            random,
            new VersionType[] { VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE }
        );
        if (random.nextBoolean()) {
            routing = randomString(random);
        }
        return new IngestDocument(index, id, version, routing, versionType, source);
    }

    public static Map<String, Object> randomSource(Random random) {
        Map<String, Object> document = new HashMap<>();
        addRandomFields(random, document, 0);
        return document;
    }

    /**
     * Generates a random field value, can be a string, a number, a list of an object itself.
     */
    public static Object randomFieldValue(Random random) {
        return randomFieldValue(random, 0);
    }

    private static Object randomFieldValue(Random random, int currentDepth) {
        switch (RandomNumbers.randomIntBetween(random, 0, 9)) {
            case 0:
                return randomString(random);
            case 1:
                return random.nextInt();
            case 2:
                return random.nextBoolean();
            case 3:
                return random.nextDouble();
            case 4:
                List<String> stringList = new ArrayList<>();
                int numStringItems = RandomNumbers.randomIntBetween(random, 1, 10);
                for (int j = 0; j < numStringItems; j++) {
                    stringList.add(randomString(random));
                }
                return stringList;
            case 5:
                List<Integer> intList = new ArrayList<>();
                int numIntItems = RandomNumbers.randomIntBetween(random, 1, 10);
                for (int j = 0; j < numIntItems; j++) {
                    intList.add(random.nextInt());
                }
                return intList;
            case 6:
                List<Boolean> booleanList = new ArrayList<>();
                int numBooleanItems = RandomNumbers.randomIntBetween(random, 1, 10);
                for (int j = 0; j < numBooleanItems; j++) {
                    booleanList.add(random.nextBoolean());
                }
                return booleanList;
            case 7:
                List<Double> doubleList = new ArrayList<>();
                int numDoubleItems = RandomNumbers.randomIntBetween(random, 1, 10);
                for (int j = 0; j < numDoubleItems; j++) {
                    doubleList.add(random.nextDouble());
                }
                return doubleList;
            case 8:
                Map<String, Object> newNode = new HashMap<>();
                addRandomFields(random, newNode, ++currentDepth);
                return newNode;
            case 9:
                byte[] byteArray = new byte[RandomNumbers.randomIntBetween(random, 1, 1024)];
                random.nextBytes(byteArray);
                return byteArray;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static String randomString(Random random) {
        if (random.nextBoolean()) {
            return RandomStrings.randomAsciiOfLengthBetween(random, 1, 10);
        }
        return RandomStrings.randomUnicodeOfCodepointLengthBetween(random, 1, 10);
    }

    private static Long randomNonNegtiveLong(Random random) {
        long randomLong = random.nextLong();
        return randomLong == Long.MIN_VALUE ? 0 : Math.abs(randomLong);
    }

    private static void addRandomFields(Random random, Map<String, Object> parentNode, int currentDepth) {
        if (currentDepth > 5) {
            return;
        }
        int numFields = RandomNumbers.randomIntBetween(random, 1, 10);
        for (int i = 0; i < numFields; i++) {
            String fieldName = randomLeafFieldName(random);
            Object fieldValue = randomFieldValue(random, currentDepth);
            parentNode.put(fieldName, fieldValue);
        }
    }
}
