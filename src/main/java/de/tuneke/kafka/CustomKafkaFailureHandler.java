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

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

public class CustomKafkaFailureHandler implements KafkaFailureHandler {

    @ApplicationScoped
    @Identifier("custom")
    public static class Factory implements KafkaFailureHandler.Factory {

        @Override
        public KafkaFailureHandler create(KafkaConnectorIncomingConfiguration config, Vertx vertx,
                                          KafkaConsumer<?, ?> consumer, BiConsumer<Throwable, Boolean> reportFailure) {
            return new CustomKafkaFailureHandler(config.getChannel(), reportFailure);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomKafkaFailureHandler.class);

    private final String channel;

    private final BiConsumer<Throwable, Boolean> reportFailure;

    public CustomKafkaFailureHandler(String channel, BiConsumer<Throwable, Boolean> reportFailure) {
        this.channel = channel;
        this.reportFailure = reportFailure;
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {
        LOGGER.warn("Processing record on channel {} of partition {} with offset {} failed.", channel,
                record.getPartition(), record.getOffset());
        // propagate failure reason
        reportFailure.accept(reason, false);
        return Uni.createFrom().<Void>failure(reason).emitOn(record::runOnMessageContext);
    }
}
