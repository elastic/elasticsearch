/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.search.spans;


import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

abstract class ContainSpans extends ConjunctionSpans {
  Spans sourceSpans;
  Spans bigSpans;
  Spans littleSpans;

  ContainSpans(Spans bigSpans, Spans littleSpans, Spans sourceSpans) {
    super(Arrays.asList(bigSpans, littleSpans));
    this.bigSpans = Objects.requireNonNull(bigSpans);
    this.littleSpans = Objects.requireNonNull(littleSpans);
    this.sourceSpans = Objects.requireNonNull(sourceSpans);
  }

  @Override
  public int startPosition() { 
    return atFirstInCurrentDoc ? -1
            : oneExhaustedInCurrentDoc ? NO_MORE_POSITIONS
            : sourceSpans.startPosition(); 
  }

  @Override
  public int endPosition() { 
    return atFirstInCurrentDoc ? -1
            : oneExhaustedInCurrentDoc ? NO_MORE_POSITIONS
            : sourceSpans.endPosition(); 
  }

  @Override
  public int width() {
    return sourceSpans.width();
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    bigSpans.collect(collector);
    littleSpans.collect(collector);
  }

}
