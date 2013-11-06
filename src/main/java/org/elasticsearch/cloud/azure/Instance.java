/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cloud.azure;

/**
 * Define an Azure Instance
 */
public class Instance {
    public static enum Status {
        STARTED;
    }

    private String privateIp;
    private String publicIp;
    private String publicPort;
    private Status status;
    private String name;

    public String getPrivateIp() {
        return privateIp;
    }

    public void setPrivateIp(String privateIp) {
        this.privateIp = privateIp;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPublicIp() {
        return publicIp;
    }

    public void setPublicIp(String publicIp) {
        this.publicIp = publicIp;
    }

    public String getPublicPort() {
        return publicPort;
    }

    public void setPublicPort(String publicPort) {
        this.publicPort = publicPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Instance instance = (Instance) o;

        if (name != null ? !name.equals(instance.name) : instance.name != null) return false;
        if (privateIp != null ? !privateIp.equals(instance.privateIp) : instance.privateIp != null) return false;
        if (publicIp != null ? !publicIp.equals(instance.publicIp) : instance.publicIp != null) return false;
        if (publicPort != null ? !publicPort.equals(instance.publicPort) : instance.publicPort != null) return false;
        if (status != instance.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = privateIp != null ? privateIp.hashCode() : 0;
        result = 31 * result + (publicIp != null ? publicIp.hashCode() : 0);
        result = 31 * result + (publicPort != null ? publicPort.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Instance{");
        sb.append("privateIp='").append(privateIp).append('\'');
        sb.append(", publicIp='").append(publicIp).append('\'');
        sb.append(", publicPort='").append(publicPort).append('\'');
        sb.append(", status=").append(status);
        sb.append(", name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
