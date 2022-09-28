/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery.azure.arm;

import com.azure.resourcemanager.compute.models.PowerState;

public record AzureVirtualMachine(String groupName, String name, PowerState powerState, String region, String publicIp, String privateIp) {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String groupName;
        private String name;
        private PowerState powerState;
        private String region;
        private String publicIp;
        private String privateIp;

        public Builder setGroupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setPowerState(PowerState powerState) {
            this.powerState = powerState;
            return this;
        }

        public Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder setPublicIp(String publicIp) {
            this.publicIp = publicIp;
            return this;
        }

        public Builder setPrivateIp(String privateIp) {
            this.privateIp = privateIp;
            return this;
        }

        public AzureVirtualMachine build() {
            return new AzureVirtualMachine(groupName, name, powerState, region, publicIp, privateIp);
        }
    }

}
