/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public final class LogFileStructureFinder {

    static final int MIN_SAMPLE_LINE_COUNT = 2;
    static final int IDEAL_SAMPLE_LINE_COUNT = 1000;

    private final String sampleFileName;
    private final String indexName;
    private final String typeName;

    /**
     * These need to be ordered so that the more generic formats come after the more specific ones
     */
    private final List<LogFileStructureFactory> orderedStructureFactories;

    public LogFileStructureFinder(Terminal terminal, Path beatsRepo, String sampleFileName, String indexName, String typeName)
        throws IOException {
        this.sampleFileName = sampleFileName;
        this.indexName = indexName;
        this.typeName = typeName;
        BeatsModuleStore beatsModuleStore =
            (beatsRepo != null) ? new BeatsModuleStore(beatsRepo.resolve("filebeat").resolve("module"), sampleFileName) : null;
        orderedStructureFactories = Arrays.asList(
            new JsonLogFileStructureFactory(terminal),
            new XmlLogFileStructureFactory(terminal),
            // ND-JSON will often also be valid (although utterly weird) CSV, so JSON must come before CSV
            new CsvLogFileStructureFactory(terminal),
            new TsvLogFileStructureFactory(terminal),
            new TextLogFileStructureFactory(terminal, beatsModuleStore)
        );
    }

    public void findLogFileConfigs(InputStream fromFile, Path outputDirectory) throws Exception {

        // Currently assumes file is UTF-8
        // TODO - determine best charset using a character sampling library
        String sample = sampleFile(fromFile, StandardCharsets.UTF_8, MIN_SAMPLE_LINE_COUNT, IDEAL_SAMPLE_LINE_COUNT);

        LogFileStructure structure = makeBestStructure(sample);
        structure.writeConfigs(outputDirectory);
    }

    LogFileStructure makeBestStructure(String sample) throws Exception {

        for (LogFileStructureFactory factory : orderedStructureFactories) {
            if (factory.canCreateFromSample(sample)) {
                return factory.createFromSample(sampleFileName, indexName, typeName, sample);
            }
        }
        throw new Exception("Input did not match any known formats");
    }

    private String sampleFile(InputStream fromFile, Charset fileCharset, int minLines, int maxLines) throws IOException {

        int lineCount = 0;
        BufferedReader reader = new BufferedReader(new InputStreamReader(fromFile, fileCharset));

        StringBuilder sample = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null && ++lineCount <= maxLines) {
            sample.append(line).append('\n');
        }

        if (lineCount < minLines) {
            throw new IOException("Input contained too few lines to sample");
        }

        return sample.toString();
    }
}
