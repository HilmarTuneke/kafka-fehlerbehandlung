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

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class Controller {
    private int counter = 0;

    public void process(String payload) {
        if(counter < 5) {
            counter++;
            throw new RuntimeException("Test error");
        }
        else {
            counter = 0;
        }
    }

    public boolean shouldSkip(String key) {
        return false;
    }

    public void addKeyToSkip(String key) {
    }
}
