/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.alerting

import com.amazon.opendistroforelasticsearch.alerting.model.Alert
import com.amazon.opendistroforelasticsearch.alerting.model.Monitor
import com.amazon.opendistroforelasticsearch.alerting.model.Trigger
import com.amazon.opendistroforelasticsearch.alerting.model.TriggerRunResult
import com.amazon.opendistroforelasticsearch.alerting.script.TriggerExecutionContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.script.ScriptService
import org.elasticsearch.search.aggregations.support.AggregationPath

/** Service that handles executing Triggers */
class TriggerService(val client: Client, val scriptService: ScriptService) {

    private val logger = LogManager.getLogger(TriggerService::class.java)

    fun isTriggerActionable(ctx: TriggerExecutionContext, result: TriggerRunResult): Boolean {
        // Suppress actions if the current alert is acknowledged and there are no errors.
        val suppress = ctx.alert?.state == Alert.State.ACKNOWLEDGED && result.error == null && ctx.error == null
        return result.triggered && !suppress
    }

    fun runTrigger(monitor: Monitor, trigger: Trigger, ctx: TriggerExecutionContext): TriggerRunResult {
        return try {
            val bucketIndices = ((ctx.results[0]["aggregations"] as HashMap<*,*>)["test-trigger"] as HashMap<*,*>)["bucket_indices"] as List<*>
            val parentBucketPath = ((ctx.results[0]["aggregations"] as HashMap<*,*>)["test-trigger"] as HashMap<*,*>)["parent_bucket_path"] as String
            val aggregationPath = AggregationPath.parse(parentBucketPath)
            //TODO test this part by passing sub-aggregation path
            var parentAgg = (ctx.results[0].get("aggregations") as HashMap<*,*>)
            aggregationPath.pathElementsAsStringList.forEach {
                sub_agg ->  parentAgg = (parentAgg[sub_agg] as HashMap<*,*>)
            }
            val buckets = parentAgg["buckets"] as List<*>
            val selectedBuckets: MutableList<Any?> = ArrayList()
            for (bucketIndex in bucketIndices) {
                selectedBuckets.add(buckets[bucketIndex as Int])
            }
            TriggerRunResult(trigger.name, selectedBuckets.size > 0, null)
        } catch (e: Exception) {
            logger.info("Error running script for monitor ${monitor.id}, trigger: ${trigger.id}", e)
            // if the script fails we need to send an alert so set triggered = true
            TriggerRunResult(trigger.name, true, e)
        }
    }
}
