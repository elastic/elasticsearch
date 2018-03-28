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

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Get repositories response
 */
public class GetRepositoriesResponse extends ActionResponse implements Iterable<RepositoryMetaData> {

    private List<RepositoryMetaData> repositories = Collections.emptyList();


    GetRepositoriesResponse() {
    }

    GetRepositoriesResponse(List<RepositoryMetaData> repositories) {
        this.repositories = repositories;
    }

    /**
     * List of repositories to return
     *
     * @return list or repositories
     */
    public List<RepositoryMetaData> repositories() {
        return repositories;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        List<RepositoryMetaData> repositoryListBuilder = new ArrayList<>(size);
        for (int j = 0; j < size; j++) {
            repositoryListBuilder.add(new RepositoryMetaData(
                    in.readString(),
                    in.readString(),
                    Settings.readSettingsFromStream(in))
            );
        }
        repositories = Collections.unmodifiableList(repositoryListBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(repositories.size());
        for (RepositoryMetaData repository : repositories) {
            out.writeString(repository.name());
            out.writeString(repository.type());
            Settings.writeSettingsToStream(repository.settings(), out);
        }
    }

    /**
     * Iterator over the repositories data
     *
     * @return iterator over the repositories data
     */
    @Override
    public Iterator<RepositoryMetaData> iterator() {
        return repositories.iterator();
    }
}
