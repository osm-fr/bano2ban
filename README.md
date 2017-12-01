# bano2ban
Script de versement de données vers l'API de gestion de la BAN

Seules les données non ODbL sont traitées pour respecter les contraintes de la BAN.

## Premier test: opendata Montpellier

Le script vérifie:
- le nom du group (voie/lieu-dit) et le corrige si
  - le libellé est uniquement en MAJUSCULES dans la BAN
  - le libellé est de longueur différente
- la présente des housenumber (numéros)
  - les ajoute si absent
  - change l'ordinal (bis/ter, etc) si il est abrégé dans la BAN et pas dans la source
