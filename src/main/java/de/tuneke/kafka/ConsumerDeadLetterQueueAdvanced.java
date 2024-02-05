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

import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ConsumerDeadLetterQueueAdvanced {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDeadLetterQueueAdvanced.class);

    @Inject
    Controller controller;

    @Inject
    @Channel("dead-letter-queue")
    Emitter<String> deadLetterEmitter;

    @Incoming("aggregate-in")
    public CompletionStage<Void> consume(KafkaRecord<String, String> record) {
        LOGGER.trace("Aggregate received: {}", record.getPayload());
        try {
            if (controller.shouldSkip(record.getKey())) {
                LOGGER.warn("Record skipped: " + record);
                deadLetterEmitter.send(record);
            } else {
                controller.process(record.getPayload());
            }
        } catch (Exception e) {
            LOGGER.error("Oops, something went terribly wrong with " + record, e);
            controller.addKeyToSkip(record.getKey());
            deadLetterEmitter.send(record);
        }
        return record.ack();
    }
}
