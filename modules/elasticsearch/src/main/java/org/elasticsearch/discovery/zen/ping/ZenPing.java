/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.discovery.zen.ping;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;

import static org.elasticsearch.cluster.node.DiscoveryNode.*;

/**
 * @author kimchy (shay.banon)
 */
public interface ZenPing extends LifecycleComponent<ZenPing> {

    void setNodesProvider(DiscoveryNodesProvider nodesProvider);

    void ping(PingListener listener, TimeValue timeout) throws ElasticSearchException;

    public interface PingListener {

        void onPing(PingResponse[] pings);
    }

    public class PingResponse implements Streamable {

        private DiscoveryNode target;

        private DiscoveryNode master;

        public PingResponse() {
        }

        public PingResponse(DiscoveryNode target, DiscoveryNode master) {
            this.target = target;
            this.master = master;
        }

        public DiscoveryNode target() {
            return target;
        }

        public DiscoveryNode master() {
            return master;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            target = readNode(in);
            if (in.readBoolean()) {
                master = readNode(in);
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            target.writeTo(out);
            if (master == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                master.writeTo(out);
            }
        }

        @Override public String toString() {
            return "ping_response target [" + target + "], master [" + master + "]";
        }
    }
}
