/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.settings.Settings;

public class GeoIpDownloaderCliIT extends GeoIpDownloaderIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (ENDPOINT != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), ENDPOINT + "cli/overview.json");
        }
        return settings.build();
    }

    public void testUseGeoIpProcessorWithDownloadedDBs() {
        assumeTrue("this test can't work with CLI (some expected files are missing)", false);
    }
}
