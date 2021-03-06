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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.DoFnRunner.ReduceFnExecutor;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.CounterSet.AddCounterMutator;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.List;

/**
 * Static utility methods that provide {@link DoFnRunner} implementations.
 */
public class DoFnRunners {
  /**
   * Information about how to create output receivers and output to them.
   */
  public interface OutputManager {
    /**
     * Outputs a single element to the receiver indicated by the given {@link TupleTag}.
     */
    <T> void output(TupleTag<T> tag, WindowedValue<T> output);
  }

  /**
   * Returns a basic implementation of {@link DoFnRunner} that works for most {@link DoFn DoFns}.
   *
   * <p>It invokes {@link DoFn#processElement} for each input.
   */
  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> simpleRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SimpleDoFnRunner<>(
        options,
        fn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        addCounterMutator,
        windowingStrategy);
  }

  /**
   * Returns an implementation of {@link DoFnRunner} that handles late data dropping.
   *
   * <p>It drops elements from expired windows before they reach the underlying {@link DoFn}.
   */
  public static <K, InputT, OutputT, W extends BoundedWindow>
      DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> lateDataDroppingRunner(
          PipelineOptions options,
          ReduceFnExecutor<K, InputT, OutputT, W> reduceFnExecutor,
          SideInputReader sideInputReader,
          OutputManager outputManager,
          TupleTag<KV<K, OutputT>> mainOutputTag,
          List<TupleTag<?>> sideOutputTags,
          StepContext stepContext,
          CounterSet.AddCounterMutator addCounterMutator,
          WindowingStrategy<?, W> windowingStrategy) {
    DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> simpleDoFnRunner =
        simpleRunner(
            options,
            reduceFnExecutor.asDoFn(),
            sideInputReader,
            outputManager,
            mainOutputTag,
            sideOutputTags,
            stepContext,
            addCounterMutator,
            windowingStrategy);
    return new LateDataDroppingDoFnRunner<>(
        simpleDoFnRunner,
        windowingStrategy,
        stepContext.timerInternals(),
        reduceFnExecutor.getDroppedDueToLatenessAggregator());
  }

  public static <InputT, OutputT> DoFnRunner<InputT, OutputT> createDefault(
      PipelineOptions options,
      DoFn<InputT, OutputT> doFn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      AddCounterMutator addCounterMutator,
      WindowingStrategy<?, ?> windowingStrategy) {
    if (doFn instanceof ReduceFnExecutor) {
      @SuppressWarnings("rawtypes")
      ReduceFnExecutor fn = (ReduceFnExecutor) doFn;
      return lateDataDroppingRunner(
          options,
          fn,
          sideInputReader,
          outputManager,
          (TupleTag) mainOutputTag,
          sideOutputTags,
          stepContext,
          addCounterMutator,
          (WindowingStrategy) windowingStrategy);
    }
    return simpleRunner(
        options,
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        stepContext,
        addCounterMutator,
        windowingStrategy);
  }
}
