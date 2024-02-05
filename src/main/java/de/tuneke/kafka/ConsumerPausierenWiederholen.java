/*
Copyright 2024 Hilmar Tuneke

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package de.tuneke.kafka;

import io.smallrye.faulttolerance.api.ExponentialBackoff;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ConsumerPausierenWiederholen {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerPausierenWiederholen.class);

    @Inject
    Controller controller;

    @Incoming("aggregate-in")
    @Retry(delay = 2000L, jitter = 400L, maxRetries = -1, maxDuration = 0)
    @ExponentialBackoff(maxDelay = 2, maxDelayUnit = ChronoUnit.HOURS)
    public CompletionStage<Void> consume(Message<String> record) {
        LOGGER.trace("Aggregate received: {}", record.getPayload());
        try {
            controller.process(record.getPayload());
            return record.ack();
        } catch (Exception e) {
            LOGGER.error("Oops, something went terribly wrong with " + record.getPayload(), e);
            // nack() invokes the failure strategy of Quarkus: https://quarkus.io/blog/kafka-failure-strategy/
            // or a custom one.
            return record.nack(e);
        }
    }
}
