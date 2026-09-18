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
     * Reminder that each {@code Lucene8xCodec} class on the classpath still needs a matching
     * {@code BWCLucene8xCodec} archive wrapper. {@link BWCCodec} remaps those Lucene 8 class names
     * when wrapping 7.x segments inside a 6.x-created archive index. This is not the Elasticsearch 7
     * read-only path (server SPI names {@code Lucene80}/{@code 84}/{@code 86}/{@code 87}).
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
