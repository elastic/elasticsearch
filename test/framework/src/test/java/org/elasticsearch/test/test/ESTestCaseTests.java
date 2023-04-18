/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.test;

import junit.framework.AssertionFailedError;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.crypto.KeyGenerator;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assume.assumeThat;

public class ESTestCaseTests extends ESTestCase {

    public void testExpectThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> { throw new IllegalArgumentException("bad arg"); });
        assertEquals("bad arg", e.getMessage());

        try {
            expectThrows(IllegalArgumentException.class, () -> { throw new IllegalStateException("bad state"); });
            fail("expected assertion error");
        } catch (AssertionFailedError assertFailed) {
            assertEquals(
                "Unexpected exception type, expected IllegalArgumentException but got java.lang.IllegalStateException: bad state",
                assertFailed.getMessage()
            );
            assertNotNull(assertFailed.getCause());
            assertEquals("bad state", assertFailed.getCause().getMessage());
        }

        try {
            expectThrows(IllegalArgumentException.class, () -> {});
            fail("expected assertion error");
        } catch (AssertionFailedError assertFailed) {
            assertNull(assertFailed.getCause());
            assertEquals("Expected exception IllegalArgumentException but no exception was thrown", assertFailed.getMessage());
        }
    }

    public void testShuffleMap() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference source = RandomObjects.randomSource(random(), xContentType, 5);
        try (XContentParser parser = createParser(xContentType.xContent(), source)) {
            LinkedHashMap<String, Object> initialMap = (LinkedHashMap<String, Object>) parser.mapOrdered();

            Set<List<String>> distinctKeys = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                LinkedHashMap<String, Object> shuffledMap = shuffleMap(initialMap, Collections.emptySet());
                assertEquals("both maps should contain the same mappings", initialMap, shuffledMap);
                List<String> shuffledKeys = new ArrayList<>(shuffledMap.keySet());
                distinctKeys.add(shuffledKeys);
            }
            // out of 10 shuffling runs we expect to have at least more than 1 distinct output.
            // This is to make sure that we actually do the shuffling
            assertThat(distinctKeys.size(), greaterThan(1));
        }
    }

    public void testShuffleXContentExcludeFields() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            {
                builder.field("field1", "value1");
                builder.field("field2", "value2");
                {
                    builder.startObject("object1");
                    {
                        builder.field("inner1", "value1");
                        builder.field("inner2", "value2");
                        builder.field("inner3", "value3");
                    }
                    builder.endObject();
                }
                {
                    builder.startObject("object2");
                    {
                        builder.field("inner4", "value4");
                        builder.field("inner5", "value5");
                        builder.field("inner6", "value6");
                    }
                    builder.endObject();
                }
            }
            builder.endObject();
            BytesReference bytes = BytesReference.bytes(builder);
            final LinkedHashMap<String, Object> initialMap;
            try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
                initialMap = (LinkedHashMap<String, Object>) parser.mapOrdered();
            }

            List<String> expectedInnerKeys1 = Arrays.asList("inner1", "inner2", "inner3");
            Set<List<String>> distinctTopLevelKeys = new HashSet<>();
            Set<List<String>> distinctInnerKeys2 = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
                    try (XContentBuilder shuffledBuilder = shuffleXContent(parser, randomBoolean(), "object1")) {
                        try (XContentParser shuffledParser = createParser(shuffledBuilder)) {
                            Map<String, Object> shuffledMap = shuffledParser.mapOrdered();
                            assertEquals("both maps should contain the same mappings", initialMap, shuffledMap);
                            List<String> shuffledKeys = new ArrayList<>(shuffledMap.keySet());
                            distinctTopLevelKeys.add(shuffledKeys);
                            @SuppressWarnings("unchecked")
                            Map<String, Object> innerMap1 = (Map<String, Object>) shuffledMap.get("object1");
                            List<String> actualInnerKeys1 = new ArrayList<>(innerMap1.keySet());
                            assertEquals("object1 should have been left untouched", expectedInnerKeys1, actualInnerKeys1);
                            @SuppressWarnings("unchecked")
                            Map<String, Object> innerMap2 = (Map<String, Object>) shuffledMap.get("object2");
                            List<String> actualInnerKeys2 = new ArrayList<>(innerMap2.keySet());
                            distinctInnerKeys2.add(actualInnerKeys2);
                        }
                    }
                }
            }

            // out of 10 shuffling runs we expect to have at least more than 1 distinct output for both top level keys and inner object2
            assertThat(distinctTopLevelKeys.size(), greaterThan(1));
            assertThat(distinctInnerKeys2.size(), greaterThan(1));
        }
    }

    public void testRandomUniqueNotUnique() {
        assertThat(randomUnique(() -> 1, 10), hasSize(1));
    }

    public void testRandomUniqueTotallyUnique() {
        AtomicInteger i = new AtomicInteger();
        assertThat(randomUnique(i::incrementAndGet, 100), hasSize(100));
    }

    public void testRandomUniqueNormalUsageAlwayMoreThanOne() {
        assertThat(randomUnique(() -> randomAlphaOfLengthBetween(1, 20), 10), hasSize(greaterThan(0)));
    }

    public void testRandomNonEmptySubsetOfThrowsOnEmptyCollection() {
        final var ex = expectThrows(IllegalArgumentException.class, () -> randomNonEmptySubsetOf(Collections.emptySet()));
        assertThat(ex.getMessage(), equalTo("Can't pick non-empty subset of an empty collection"));
    }

    public void testRandomNonNegativeLong() {
        assertThat(randomNonNegativeLong(), greaterThanOrEqualTo(0L));
    }

    public void testRandomNonNegativeInt() {
        assertThat(randomNonNegativeInt(), greaterThanOrEqualTo(0));
    }

    public void testRandomValueOtherThan() {
        // "normal" way of calling where the value is not null
        int bad = randomInt();
        assertNotEquals(bad, (int) randomValueOtherThan(bad, ESTestCase::randomInt));

        /*
         * "funny" way of calling where the value is null. This once
         * had a unique behavior but at this point `null` acts just
         * like any other value.
         */
        Supplier<Object> usuallyNull = () -> usually() ? null : randomInt();
        assertNotNull(randomValueOtherThan(null, usuallyNull));
    }

    public void testWorkerSystemProperty() {
        assumeTrue("requires running tests with Gradle", System.getProperty("tests.gradle") != null);

        assertThat(ESTestCase.TEST_WORKER_VM_ID, not(equals(ESTestCase.DEFAULT_TEST_WORKER_ID)));
    }

    public void testBasePortGradle() {
        assumeTrue("requires running tests with Gradle", System.getProperty("tests.gradle") != null);
        // Gradle worker IDs are 1 based
        assertNotEquals(ESTestCase.MIN_PRIVATE_PORT, ESTestCase.getWorkerBasePort());
    }

    public void testBasePortIDE() {
        assumeTrue("requires running tests without Gradle", System.getProperty("tests.gradle") == null);
        assertEquals(ESTestCase.MIN_PRIVATE_PORT, ESTestCase.getWorkerBasePort());
    }

    public void testRandomDateFormatterPattern() {
        DateFormatter formatter = DateFormatter.forPattern(randomDateFormatterPattern());
        /*
         * Make sure it doesn't crash trying to format some dates and
         * that round tripping through millis doesn't lose any information.
         * Interestingly, round tripping through a string *can* lose
         * information because not all date formats spit out milliseconds.
         * Hell, not all of them spit out the time of day at all!
         * But going from text back to millis back to text should
         * be fine!
         */
        String formatted = formatter.formatMillis(randomLongBetween(0, 2_000_000_000_000L));
        String formattedAgain = formatter.formatMillis(formatter.parseMillis(formatted));
        assertThat(formattedAgain, equalTo(formatted));
    }

    public void testSkipTestWaitingForLuceneFix() {
        // skip test when Lucene fix has not been integrated yet
        AssumptionViolatedException ave = expectThrows(
            AssumptionViolatedException.class,
            () -> skipTestWaitingForLuceneFix(Version.fromBits(99, 0, 0), "extra message")
        );
        assertThat(ave.getMessage(), containsString("Skipping test as it is waiting on a Lucene fix: extra message"));

        // fail test when it still calls skipTestWaitingForLuceneFix() after Lucene fix has been integrated
        AssertionError ae = expectThrows(
            AssertionError.class,
            () -> skipTestWaitingForLuceneFix(Version.fromBits(1, 0, 0), "extra message")
        );
        assertThat(ae.getMessage(), containsString("Remove call of skipTestWaitingForLuceneFix"));
    }

    public void testSecureRandom() throws NoSuchAlgorithmException, NoSuchProviderException {
        assumeThat(ESTestCase.inFipsJvm(), is(false));
        final int numInstances = 2;
        final int numTries = 3;
        for (int numInstance = 0; numInstance < numInstances; numInstance++) {
            final byte[] seed = randomByteArrayOfLength(randomIntBetween(16, 32));
            final SecureRandom secureRandom1 = secureRandom(seed);
            final SecureRandom secureRandom2 = secureRandom(seed);
            // verify both SecureRandom instances produce deterministic output
            for (int numTry = 0; numTry < numTries; numTry++) {
                final int bound = randomIntBetween(16, 1024);
                assertThat(secureRandom1.nextBoolean(), is(equalTo(secureRandom2.nextBoolean())));
                assertThat(secureRandom1.nextInt(), is(equalTo(secureRandom2.nextInt())));
                assertThat(secureRandom1.nextInt(bound), is(equalTo(secureRandom2.nextInt(bound))));
                assertThat(secureRandom1.nextLong(), is(equalTo(secureRandom2.nextLong())));
                assertThat(secureRandom1.nextLong(bound), is(equalTo(secureRandom2.nextLong(bound))));
                assertThat(secureRandom1.nextFloat(), is(equalTo(secureRandom2.nextFloat())));
                assertThat(secureRandom1.nextDouble(), is(equalTo(secureRandom2.nextDouble())));
                assertThat(secureRandom1.nextGaussian(), is(equalTo(secureRandom2.nextGaussian())));
                assertThat(secureRandom1.nextExponential(), is(equalTo(secureRandom2.nextExponential())));
                final byte[] randomBytes1 = new byte[bound];
                final byte[] randomBytes2 = new byte[bound];
                secureRandom1.nextBytes(randomBytes1);
                secureRandom2.nextBytes(randomBytes2);
                assertThat(randomBytes1, is(equalTo(randomBytes2)));
            }
        }
    }

    public void testSecureRandomNonFipsMode() {
        assumeThat(ESTestCase.inFipsJvm(), is(false));
        final NoSuchAlgorithmException exception = expectThrows(NoSuchAlgorithmException.class, ESTestCase::secureRandomFips);
        assertThat(exception.getMessage(), is(equalTo("DEFAULT SecureRandom not available")));
    }

    public void testSecureRandomFipsMode() throws NoSuchAlgorithmException {
        assumeThat(ESTestCase.inFipsJvm(), is(true));
        // Non-FIPS SHA1PRNG/SUN works in FIPS mode
        final SecureRandom secureRandomNonFips1 = secureRandomNonFips();
        assertThat(secureRandomNonFips1, is(notNullValue()));
        assertThat(secureRandomNonFips1.getProvider(), is(notNullValue()));
        assertThat(secureRandomNonFips1.getProvider().getName(), is(equalTo("SUN")));
        // FIPS DEFAULT/BCFIPS works in FIPS mode. If just running this method, this is the first call and triggers a log warning.
        final SecureRandom secureRandomFips1 = secureRandomFips(); // FIPS SecureRandom works, logs warning
        assertThat(secureRandomFips1, is(notNullValue()));
        assertThat(secureRandomFips1.getProvider(), is(notNullValue()));
        assertThat(secureRandomFips1.getProvider().getName(), is(equalTo("BCFIPS")));
        // If just running this method, this is the second call and does not trigger a log warning.
        final SecureRandom secureRandomFips2 = secureRandomFips(); // FIPS SecureRandom works, does not log warning
        assertThat(secureRandomFips2, is(notNullValue()));
        assertThat(secureRandomFips2.getProvider(), is(notNullValue()));
        assertThat(secureRandomFips2.getProvider().getName(), is(equalTo("BCFIPS")));
    }

    public void testNonFipsKeyGenWithNonFipsSecureRandom() throws NoSuchAlgorithmException {
        assumeThat(ESTestCase.inFipsJvm(), is(false));
        // Non-FIPS SHA1PRNG/SUN
        final SecureRandom secureRandomNonFips = secureRandomNonFips();
        assertThat(secureRandomNonFips, is(notNullValue()));
        assertThat(secureRandomNonFips.getProvider(), is(notNullValue()));
        assertThat(secureRandomNonFips.getProvider().getName(), is(equalTo("SUN")));
        // Non-FIPS AES/SunJCE
        final KeyGenerator keyGeneratorNonFips = KeyGenerator.getInstance("AES");
        assertThat(keyGeneratorNonFips, is(notNullValue()));
        assertThat(keyGeneratorNonFips.getProvider(), is(notNullValue()));
        assertThat(keyGeneratorNonFips.getProvider().getName(), is(equalTo("SunJCE")));
        // non-FIPS KeyGenerator.init() works with non-FIPS SecureRandom
        keyGeneratorNonFips.init(256, secureRandomNonFips);
        assertThat(keyGeneratorNonFips.generateKey(), is(notNullValue()));
    }

    public void testFipsKeyGenWithFipsSecureRandom() throws NoSuchAlgorithmException {
        assumeThat(ESTestCase.inFipsJvm(), is(true));
        // FIPS DEFAULT/BCFIPS
        final SecureRandom secureRandomFips = secureRandomFips(); // FIPS SecureRandom works
        assertThat(secureRandomFips, is(notNullValue()));
        assertThat(secureRandomFips.getProvider(), is(notNullValue()));
        assertThat(secureRandomFips.getProvider().getName(), is(equalTo("BCFIPS")));
        // FIPS AES/BCFIPS
        final KeyGenerator keyGeneratorFips = KeyGenerator.getInstance("AES");
        assertThat(keyGeneratorFips, is(notNullValue()));
        assertThat(keyGeneratorFips.getProvider(), is(notNullValue()));
        assertThat(keyGeneratorFips.getProvider().getName(), is(equalTo("BCFIPS")));
        // FIPS KeyGenerator.init() works with FIPS SecureRandom
        keyGeneratorFips.init(256, secureRandomFips);
        assertThat(keyGeneratorFips.generateKey(), is(notNullValue()));
    }

    public void testFipsKeyGenWithNonFipsSecureRandom() {
        assumeThat(ESTestCase.inFipsJvm(), is(true));
        final Exception wrappedThrowable = expectThrows(Exception.class, () -> {
            // Non-FIPS SHA1PRNG/SUN
            final SecureRandom secureRandomNonFips = secureRandomNonFips(); // non-FIPS SecureRandom works
            assertThat(secureRandomNonFips, is(notNullValue()));
            assertThat(secureRandomNonFips.getProvider(), is(notNullValue()));
            assertThat(secureRandomNonFips.getProvider().getName(), is(equalTo("SUN")));
            // FIPS DEFAULT/BCFIPS
            final KeyGenerator keyGeneratorFips = KeyGenerator.getInstance("AES");
            assertThat(keyGeneratorFips, is(notNullValue()));
            assertThat(keyGeneratorFips.getProvider(), is(notNullValue()));
            assertThat(keyGeneratorFips.getProvider().getName(), is(equalTo("BCFIPS")));
            try {
                // FIPS KeyGenerator.init() rejects non-FIPS SecureRandom
                keyGeneratorFips.init(256, secureRandomNonFips);
            } catch (Throwable t) {
                // wrap Throwable, since expectThrows can only catch Exception (FipsUnapprovedOperationError), not Throwable
                throw new Exception(t);
            }
        });
        final Throwable t = wrappedThrowable.getCause(); // unwrap Throwable (FipsUnapprovedOperationError)
        assertThat(t.getClass().getCanonicalName(), is(equalTo("org.bouncycastle.crypto.fips.FipsUnapprovedOperationError")));
        assertThat(t.getMessage(), is(equalTo("Attempt to create key with unapproved RNG: AES")));
    }
}
