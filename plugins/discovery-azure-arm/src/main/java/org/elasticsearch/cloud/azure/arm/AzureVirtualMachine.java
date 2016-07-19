/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cloud.azure.arm;

import com.microsoft.azure.management.compute.PowerState;

/**
 * Represents an Azure Virtual Machine
 */
public class AzureVirtualMachine {

    /**
     * Power State. We are using here our own version so we can move more seamlessly if
     * Azure SDK changed something.
     */
    public enum PowerState {
        RUNNING(com.microsoft.azure.management.compute.PowerState.RUNNING),
        DEALLOCATING(com.microsoft.azure.management.compute.PowerState.DEALLOCATING),
        DEALLOCATED(com.microsoft.azure.management.compute.PowerState.DEALLOCATED),
        STARTING(com.microsoft.azure.management.compute.PowerState.STARTING),
        STOPPED(com.microsoft.azure.management.compute.PowerState.STOPPED),
        UNKNOWN(com.microsoft.azure.management.compute.PowerState.UNKNOWN);

        private com.microsoft.azure.management.compute.PowerState powerState ;

        PowerState(com.microsoft.azure.management.compute.PowerState powerState) {
            this.powerState = powerState ;
        }

        public static PowerState fromAzurePowerState(com.microsoft.azure.management.compute.PowerState azurePowerState) {
            if (azurePowerState == null) {
                return UNKNOWN;
            }
            for (PowerState powerState : values()) {
                if (powerState.powerState.equals(azurePowerState)) {
                    return powerState;
                }
            }
            throw new IllegalArgumentException("invalid value for power state [" + azurePowerState + "]");
        }
    }

    private String groupName;
    private String name;
    private String region;
    private String publicIp;
    private String privateIp;
    private PowerState powerState;

    // TODO Add metadata so people will be able to define on which port elasticsearch is actually running

    public String getGroupName() {
        return groupName;
    }

    void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    AzureVirtualMachine withGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    AzureVirtualMachine withName(String name) {
        this.name = name;
        return this;
    }

    public String getRegion() {
        return region;
    }

    void setRegion(String region) {
        this.region = region;
    }

    AzureVirtualMachine withRegion(String region) {
        this.region = region;
        return this;
    }

    public String getPublicIp() {
        return publicIp;
    }

    void setPublicIp(String publicIp) {
        this.publicIp = publicIp;
    }

    AzureVirtualMachine withPublicIp(String publicIp) {
        this.publicIp = publicIp;
        return this;
    }

    public String getPrivateIp() {
        return privateIp;
    }

    void setPrivateIp(String privateIp) {
        this.privateIp = privateIp;
    }

    AzureVirtualMachine withPrivateIp(String privateIp) {
        this.privateIp = privateIp;
        return this;
    }

    public PowerState getPowerState() {
        return powerState;
    }

    void setPowerState(PowerState powerState) {
        this.powerState = powerState;
    }

    AzureVirtualMachine withPowerState(PowerState powerState) {
        this.powerState = powerState;
        return this;
    }

    @Override
    public String toString() {
        return "AzureVirtualMachine{" + "groupName='" + groupName + '\'' +
            ", name='" + name + '\'' +
            ", region='" + region + '\'' +
            ", publicIp='" + publicIp + '\'' +
            ", privateIp='" + privateIp + '\'' +
            ", powerState='" + powerState + '\'' +
            '}';
    }
}
