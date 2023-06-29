package com.haibui.demominio.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ItemDTO {
    private String etag;
    private String objectName;

    // Other attributes
}
