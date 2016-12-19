/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.integration;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.ValueProvider;
import com.google.cloud.dataflow.sdk.options.ValueProvider.NestedValueProvider;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.util.Transport;

import java.io.IOException;

public class PubSubToBigQuery {
  /**
   * Options supported by {@link PubSubToBigQuery}.
   */
  public interface Options extends PipelineOptions {
    @Description("Table spec to write the output to")
    ValueProvider<String> getOutputTableSpec();
    void setOutputTableSpec(ValueProvider<String> value);

    @Description("Pub/Sub topic to read the input from")
    ValueProvider<String> getInputTopic();
    void setInputTopic(ValueProvider<String> value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(Options.class);
    Pipeline pipeline = Pipeline.create(options);
    final ValueProvider<String> tableSpec = options.getOutputTableSpec();

    pipeline
        .apply("ReadPubsub", PubsubIO.Read.topic(options.getInputTopic()))
        .apply("ConvertToRow", MapElements.via(
            new SimpleFunction<String, TableRow>() {
              @Override
              public TableRow apply(String input) {
                try {
                  return Transport.getJsonFactory().fromString(input, TableRow.class);
                } catch (IOException e) {
                  throw new RuntimeException("Unable to parse input", e);
                }
              }
            }))
        .apply("WriteBigQuery", BigQueryIO.Write
            .withoutValidation()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .to(tableSpec));

    pipeline.run();
  }
}
