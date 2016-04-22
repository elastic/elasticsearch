/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.graph.GraphLicensee;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeTestCase;

import static org.hamcrest.Matchers.is;

public class LicenseTests extends AbstractLicenseeTestCase {

    private SimpleLicenseeRegistry licenseeRegistry = new SimpleLicenseeRegistry();

    public void testPlatinumTrialLicenseCanDoEverything() throws Exception {
        licenseeRegistry.setOperationMode(randomTrialOrPlatinumMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    public void testBasicLicenseIsDisabled() throws Exception {
        licenseeRegistry.setOperationMode(OperationMode.BASIC);
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }

    public void testNoLicenseDoesNotWork() {
        licenseeRegistry.setOperationMode(OperationMode.BASIC);
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }

    public void testExpiredPlatinumTrialLicenseIsRestricted() throws Exception {
        licenseeRegistry.setOperationMode(randomTrialOrPlatinumMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }

    public void testUpgradingFromBasicLicenseWorks() {
        licenseeRegistry.setOperationMode(OperationMode.BASIC);
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);

        licenseeRegistry.setOperationMode(randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    public void testDowngradingToBasicLicenseWorks() {
        licenseeRegistry.setOperationMode(randomTrialOrPlatinumMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicensePlatinumTrialBehaviour(graphLicensee);

        licenseeRegistry.setOperationMode(OperationMode.BASIC);
        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }
    
    public void testDowngradingToGoldLicenseWorks() {
        licenseeRegistry.setOperationMode(randomTrialOrPlatinumMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicensePlatinumTrialBehaviour(graphLicensee);

        licenseeRegistry.setOperationMode(OperationMode.GOLD);
        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }    

    public void testUpgradingExpiredLicenseWorks() {
        licenseeRegistry.setOperationMode(randomTrialOrPlatinumMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);

        licenseeRegistry.setOperationMode(randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    private void assertLicensePlatinumTrialBehaviour(GraphLicensee graphLicensee) {
        assertThat("Expected graph exploration to be allowed", graphLicensee.isAvailable(), is(true));
    }

    private void assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(GraphLicensee graphLicensee) {
        assertThat("Expected graph exploration not to be allowed", graphLicensee.isAvailable(), is(false));
    }
}
