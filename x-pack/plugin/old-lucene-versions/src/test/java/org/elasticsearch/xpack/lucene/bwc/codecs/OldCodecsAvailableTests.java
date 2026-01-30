/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs;

import org.apache.lucene.codecs.Codec;
import org.elasticsearch.Version;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.test.ESTestCase;

import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OldCodecsAvailableTests extends ESTestCase {

    /**
     * This test verifies for each Lucene codec available via SPI; we also provide a corresponding BWC codec counterpart.
     * Using a ServiceLoader, we fetch all classes matching the codecPathRegex (this is applied for Lucne8xCodec at the moment).
     * For each entry of the returned list, we intend to load the BWC counterpart reflectively.
     *
     * Reminder to add Lucene BWC codecs under {@link org.elasticsearch.xpack.lucene.bwc.codecs} whenever Elasticsearch is upgraded
     * to the next major Lucene version.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.SEARCH_FOUNDATIONS)
    public void testLuceneBWCCodecsAvailable() {
        assertEquals("Add Lucene BWC codecs for Elasticsearch version 8", 9, Version.CURRENT.major);

        String codecPathRegex = ".*[\\\\.](Lucene(8[0-9])Codec)";
        Pattern codecPathPattern = Pattern.compile(codecPathRegex);

        String codecClassNameRegex = "Lucene(\\d+)Codec";
        Pattern classNamePattern = Pattern.compile(codecClassNameRegex);

        for (Codec codec : ServiceLoader.load(Codec.class)) {
            Matcher codecPathMatcher = codecPathPattern.matcher(codec.getClass().getName());
            if (codecPathMatcher.matches()) {
                String pathName = codec.getClass().getName();
                int lastDotIndex = pathName.lastIndexOf('.');
                String className = pathName.substring(lastDotIndex + 1);

                Matcher classNameMatcher = classNamePattern.matcher(className);
                if (classNameMatcher.matches()) {
                    String codecVersion = classNameMatcher.group(1);
                    String wrappedCodecClassPath = "org.elasticsearch.xpack.lucene.bwc.codecs.lucene"
                        + codecVersion
                        + ".BWCLucene"
                        + codecVersion
                        + "Codec";
                    assertTrue(isClassPresent(wrappedCodecClassPath));
                }
            }
        }
    }

    private static boolean isClassPresent(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
