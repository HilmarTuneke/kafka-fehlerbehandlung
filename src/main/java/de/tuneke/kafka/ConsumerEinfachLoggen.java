package de.tuneke.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ConsumerEinfachLoggen {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerEinfachLoggen.class);

    @Inject
    Controller controller;

    @Incoming("aggregate-in")
    public CompletionStage<Void> consume(Message<String> record) {
        LOGGER.trace("Aggregate received: {}", record.getPayload());
        try {
            controller.process(record.getPayload());
        } catch (Exception e) {
            LOGGER.error("Oops, something went terribly wrong with " + record.getPayload(), e);
        }
        return record.ack();
    }

}
