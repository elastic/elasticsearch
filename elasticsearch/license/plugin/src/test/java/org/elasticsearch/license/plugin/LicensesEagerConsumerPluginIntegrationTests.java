/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationConsumerPlugin;

// test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
public class LicensesEagerConsumerPluginIntegrationTests extends AbstractLicensesConsumerPluginIntegrationTestCase {

    public LicensesEagerConsumerPluginIntegrationTests() {
        super(new EagerLicenseRegistrationConsumerPlugin(Settings.EMPTY));
    }
}
