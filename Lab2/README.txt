файл x_lab_send делает предобработку файлов, разбивает их на строки и начинает передачу в кафка-потоке main
файл x_lab receive получает данный поток, удаляет из всех сообщений стоп-слова и на основе информации и сообщении отправляет его в соответствующий подпоток
Таким образом принятый поток main разделяется на несколько подпотоков по полу и возрасту
Фвйл x_lab receive copy содержит consumer. подписанный на один из подпотоков. 
Приняв с помощью него данные, мы считываем их спарком в датафрейм, производим группировку по нужному окну, упаковываем вновь эти данные
и передаем через соответствующего producer, попутно напечатав содерржание посылки (сообщения)

------------
файлы lab2_ producer && lab2_consumer вариант лабы от Даниила Нелюбина

------------
файлы Consumer && Prosucer содержат вариант лабы от Андрея Антонова
