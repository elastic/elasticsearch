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
package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response acknowledging the Machine Learning Job request
 */
public class DeleteJobResponse extends AcknowledgedResponse {

   public DeleteJobResponse(boolean acknowledged) {
       super(acknowledged);
   }

   public DeleteJobResponse() {
   }

    public static DeleteJobResponse fromXContent(XContentParser parser) throws IOException {
        AcknowledgedResponse response = AcknowledgedResponse.fromXContent(parser);
        return new DeleteJobResponse(response.isAcknowledged());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DeleteJobResponse that = (DeleteJobResponse) other;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAcknowledged());
    }

}
