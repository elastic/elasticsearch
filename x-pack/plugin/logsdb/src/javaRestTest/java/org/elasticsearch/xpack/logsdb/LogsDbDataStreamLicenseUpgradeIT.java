/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LogsDbDataStreamLicenseUpgradeIT extends LogsDbDataStreamLicenseChangeIT {
    @Override
    protected void licenseChange() throws IOException {
        startTrial();
    }

    protected List<String> prepareDataStreams() throws IOException {
        var dataStreams = new ArrayList<String>();

        assertOK(createDataStream(client(), "logs-test-regular"));
        dataStreams.add("logs-test-regular");

        var sourceModeOverride = """
            {
              "template": {
                "settings": {
                  "index": {
                    "mapping.source.mode": "SYNTHETIC"
                  }
                }
              }
            }""";
        assertOK(putComponentTemplate(client(), "logs@custom", sourceModeOverride));
        assertOK(createDataStream(client(), "logs-test-custom"));
        assertOK(removeComponentTemplate(client(), "logs@custom"));
        dataStreams.add("logs-test-custom");

        return dataStreams;
    }

    @Override
    protected SourceFieldMapper.Mode initialMode() {
        return SourceFieldMapper.Mode.STORED;
    }

    @Override
    protected SourceFieldMapper.Mode finalMode() {
        return SourceFieldMapper.Mode.SYNTHETIC;
    }
}
