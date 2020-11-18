package com.example.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class HelloController {
    @GetMapping
    public ModelAndView hello(Model model) {
        String message =  "Hello, user!";

        return new ModelAndView("index")
                .addObject("message", message);
    }
}
