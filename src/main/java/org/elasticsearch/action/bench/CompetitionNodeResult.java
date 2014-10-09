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

package org.elasticsearch.action.bench;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Per-node result of competition iterations
 */
public class CompetitionNodeResult extends ActionResponse implements Streamable {

    private String competitionName;
    private String nodeName;
    private int totalIterations = 0;
    private int completedIterations = 0;
    private int totalExecutedQueries = 0;
    private long warmUpTime = 0;
    private List<CompetitionIteration> iterations;

    public CompetitionNodeResult() {
        iterations = new ArrayList<>();
    }

    public CompetitionNodeResult(String competitionName, String nodeName, int totalIterations, List<CompetitionIteration> iterations) {
        this.competitionName = competitionName;
        this.nodeName = nodeName;
        this.iterations = iterations;
        this.totalIterations = totalIterations;
    }

    public String competitionName() {
        return competitionName;
    }

    public String nodeName() {
        return nodeName;
    }

    public int totalIterations() {
        return totalIterations;
    }

    public int completedIterations() {
        return completedIterations;
    }

    public void incrementCompletedIterations() {
        completedIterations++;
    }

    public long warmUpTime() {
        return warmUpTime;
    }

    public void warmUpTime(long warmUpTime) {
        this.warmUpTime = warmUpTime;
    }

    public int totalExecutedQueries() {
        return totalExecutedQueries;
    }

    public void totalExecutedQueries(int totalExecutedQueries) {
        this.totalExecutedQueries = totalExecutedQueries;
    }

    public List<CompetitionIteration> iterations() {
        return iterations;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        competitionName = in.readString();
        nodeName = in.readString();
        totalIterations = in.readVInt();
        completedIterations = in.readVInt();
        totalExecutedQueries = in.readVInt();
        warmUpTime = in.readVLong();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            CompetitionIteration iteration = new CompetitionIteration();
            iteration.readFrom(in);
            iterations.add(iteration);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(competitionName);
        out.writeString(nodeName);
        out.writeVInt(totalIterations);
        out.writeVInt(completedIterations);
        out.writeVInt(totalExecutedQueries);
        out.writeVLong(warmUpTime);
        out.writeVInt(iterations.size());
        for (CompetitionIteration iteration : iterations) {
            iteration.writeTo(out);
        }
    }
}
