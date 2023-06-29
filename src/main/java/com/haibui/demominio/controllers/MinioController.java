package com.haibui.demominio.controllers;

import com.haibui.demominio.models.ItemDTO;
import com.haibui.demominio.services.MinioService;
import io.minio.errors.MinioException;
import io.minio.messages.Item;
import jakarta.servlet.http.HttpServletResponse;
import lombok.AllArgsConstructor;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.nio.file.Path;
import java.util.List;

@RestController
@AllArgsConstructor
@RequestMapping("/api/minio")
public class MinioController {
    private MinioService minioService;
    @GetMapping("/")
    public List<Item> list() throws MinioException {
        return minioService.list();
    }

    @GetMapping("/full")
    public List<ItemDTO> fullList() throws MinioException {
        List<Item> items = minioService.fullList();
        List<ItemDTO> itemDTOS =  items.stream().map(item -> ItemDTO.builder()
                                    .etag(item.etag())
                                    .objectName(item.objectName())
                                    .build()).toList();

        for (ItemDTO item : itemDTOS) {
            System.out.println(item.getObjectName());
        }
        return itemDTOS;
    }

    @GetMapping("/{object}")
    public void getObject(@PathVariable("object") String object, HttpServletResponse response) throws MinioException, IOException {
        InputStream inputStream = minioService.get(Path.of(object));

        // Set the content type and attachment header.
        response.addHeader("Content-disposition", "attachment;filename=" + object);
        response.setContentType(URLConnection.guessContentTypeFromName(object));

        // Copy the stream to the response's output stream.
        IOUtils.copy(inputStream, response.getOutputStream());
        response.flushBuffer();
    }

    @PostMapping("/upload")
    public void addAttachement(@RequestParam("file") MultipartFile file) {
        Path path = Path.of(file.getOriginalFilename());
        try {
            minioService.upload(path, file.getInputStream(), file.getContentType());
        } catch (MinioException e) {
            throw new IllegalStateException("The file cannot be upload on the internal storage. Please retry later", e);
        } catch (IOException e) {
            throw new IllegalStateException("The file cannot be read", e);
        }
    }
}
