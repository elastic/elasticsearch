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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Administrative commands for benchmarks: pause, resume
 */
public class BenchmarkControlRequest extends AcknowledgedRequest {

    private String benchmarkName;
    private Command command = Command.STATUS;

    public enum Command {
        PAUSE((byte) 0),                    // Pause an active benchmark
        RESUME((byte) 1),                   // Resume an active benchmark
        STATUS((byte) 2);                   // Report status of active benchmarks

        private final byte id;
        private static final Command[] COMMANDS = new Command[Command.values().length];

        static {
            for (Command command : Command.values()) {
                assert command.id() < COMMANDS.length && command.id() >= 0;
                COMMANDS[command.id()] = command;
            }
        }

        Command(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Command fromId(byte id) throws ElasticsearchIllegalArgumentException {
            if (id < 0 || id > values().length) {
                throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
            }
            return COMMANDS[id];
        }
    }

    public BenchmarkControlRequest() { }

    public BenchmarkControlRequest(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    public void benchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    public String benchmarkName() {
        return benchmarkName;
    }

    public void command(Command command) {
        this.command = command;
    }

    public Command command() {
        return command;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ((command == Command.PAUSE || command == Command.RESUME) && benchmarkName == null) {
            validationException = ValidateActions.addValidationError("benchmarkName must not be null", validationException);
        }
        if (command == null) {
            validationException = ValidateActions.addValidationError("command must not be null", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        benchmarkName = in.readOptionalString();
        command = Command.fromId(in.readByte());
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(benchmarkName);
        out.writeByte(command.id());
        writeTimeout(out);
    }
}