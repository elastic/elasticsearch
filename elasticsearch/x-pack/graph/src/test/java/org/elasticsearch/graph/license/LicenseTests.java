/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph.license;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.AbstractLicenseeTestCase;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class LicenseTests extends AbstractLicenseeTestCase {

    private SimpleLicenseeRegistry licenseeRegistry = new SimpleLicenseeRegistry();

    public void testPlatinumTrialLicenseCanDoEverything() throws Exception {
        licenseeRegistry.setOperationMode(randomAllFeaturesMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    public void testFreeLicenseIsDisabled() throws Exception {
        licenseeRegistry.setOperationMode(randomFreeMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }

    public void testNoLicenseDoesNotWork() {
        licenseeRegistry.setOperationMode(randomFreeMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }

    public void testExpiredPlatinumTrialLicenseIsRestricted() throws Exception {
        licenseeRegistry.setOperationMode(randomAllFeaturesMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }

    public void testUpgradingFromFreeLicenseWorks() {
        licenseeRegistry.setOperationMode(randomFreeMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);

        licenseeRegistry.setOperationMode(randomAllFeaturesMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    public void testDowngradingToFreeLicenseWorks() {
        licenseeRegistry.setOperationMode(randomAllFeaturesMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicensePlatinumTrialBehaviour(graphLicensee);

        licenseeRegistry.setOperationMode(randomFreeMode());
        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }
    
    public void testDowngradingToGoldLicenseWorks() {
        licenseeRegistry.setOperationMode(randomAllFeaturesMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);

        assertLicensePlatinumTrialBehaviour(graphLicensee);

        licenseeRegistry.setOperationMode(License.OperationMode.GOLD);
        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);
    }    

    public void testUpgradingExpiredLicenseWorks() {
        licenseeRegistry.setOperationMode(randomAllFeaturesMode());
        GraphLicensee graphLicensee = new GraphLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(graphLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(graphLicensee);

        licenseeRegistry.setOperationMode(randomAllFeaturesMode());
        assertLicensePlatinumTrialBehaviour(graphLicensee);
    }

    private void assertLicensePlatinumTrialBehaviour(GraphLicensee graphLicensee) {
        assertThat("Expected graph exploration to be allowed", graphLicensee.isGraphExploreEnabled(), is(true));
    }

    private void assertLicenseBasicOrGoldOrNoneOrExpiredBehaviour(GraphLicensee graphLicensee) {
        assertThat("Expected graph exploration not to be allowed", graphLicensee.isGraphExploreEnabled(), is(false));
    }

    public static class SimpleLicenseeRegistry extends AbstractComponent implements LicenseeRegistry {
        private final List<Licensee> licensees = new ArrayList<>();
        private License.OperationMode operationMode;

        public SimpleLicenseeRegistry() {
            super(Settings.EMPTY);
        }

        @Override
        public void register(Licensee licensee) {
            licensees.add(licensee);
            enable();
        }

        public void enable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(operationMode, randomActiveState()));
            }
        }

        public void disable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(operationMode, randomInactiveState()));
            }
        }

        public void setOperationMode(License.OperationMode operationMode) {
            this.operationMode = operationMode;
            enable();
        }
    }
}
