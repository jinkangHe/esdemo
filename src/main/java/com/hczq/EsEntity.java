package com.hczq;

import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * @Description
 * @Author hejinkang
 * @Date 2024/12/26 09:15
 * @Version 1.0
 */
@Data
public class EsEntity {
    private String name;
    private Integer age;

    private LocalDate birthday;

    private List<SubjectInfo> subjectInfo;

    private Integer buyTimes;
}
