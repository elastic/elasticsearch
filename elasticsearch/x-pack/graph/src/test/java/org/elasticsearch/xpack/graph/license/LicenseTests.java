/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.graph.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeTestCase;
import org.elasticsearch.xpack.graph.GraphLicensee;

import static org.hamcrest.Matchers.is;

public class LicenseTests extends AbstractLicenseeTestCase {

    GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY);

    public void testPlatinumTrialLicenseCanDoEverything() throws Exception {
        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    public void testBasicLicenseIsDisabled() throws Exception {
        setOperationMode(graphLicensee, OperationMode.BASIC);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }
    
    public void testStandardLicenseIsDisabled() throws Exception {
        setOperationMode(graphLicensee, OperationMode.STANDARD);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }    

    public void testNoLicenseDoesNotWork() {
        setOperationMode(graphLicensee, OperationMode.BASIC);
        disable(graphLicensee);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }

    public void testExpiredPlatinumTrialLicenseIsRestricted() throws Exception {
        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        disable(graphLicensee);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }

    public void testUpgradingFromBasicLicenseWorks() {
        setOperationMode(graphLicensee, OperationMode.BASIC);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);

        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    public void testDowngradingToBasicLicenseWorks() {
        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);

        setOperationMode(graphLicensee, OperationMode.BASIC);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }
    
    public void testUpgradingFromStandardLicenseWorks() {
        setOperationMode(graphLicensee, OperationMode.STANDARD);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);

        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    public void testDowngradingToStandardLicenseWorks() {
        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);

        setOperationMode(graphLicensee, OperationMode.STANDARD);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }    
    
    public void testDowngradingToGoldLicenseWorks() {
        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);

        setOperationMode(graphLicensee, OperationMode.GOLD);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }    

    public void testUpgradingExpiredLicenseWorks() {
        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        disable(graphLicensee);
        assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(graphLicensee);

        setOperationMode(graphLicensee, randomTrialOrPlatinumMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    private void assertLicensePlatinumTrialBehaviour(GraphLicensee graphLicensee) {
        assertThat("Expected graph exploration to be allowed", graphLicensee.isAvailable(), is(true));
    }

    private void assertLicenseBasicOrStandardGoldOrNoneOrExpiredBehaviour(GraphLicensee graphLicensee) {
        assertThat("Expected graph exploration not to be allowed", graphLicensee.isAvailable(), is(false));
    }
}
